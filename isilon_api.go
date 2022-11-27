package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/publicsuffix"
)

// MaxAPIPathLen is the limit on the length of an API request URL
const MaxAPIPathLen = 8198

// AuthInfo provides username and password to authenticate
// against the OneFS API
type AuthInfo struct {
	Username string
	Password string
}

// Cluster contains all of the information to talk to a OneFS
// cluster via the OneFS API
type Cluster struct {
	AuthInfo
	AuthType    string
	Hostname    string
	Port        int
	VerifySSL   bool
	OSVersion   string
	ClusterName string
	baseURL     string
	client      *http.Client
	csrfToken   string
	reauthTime  time.Time
	maxRetries  int
	normalize   bool
}

// clientSummaryResult contains the information returned for a single Client entry
// as returned by the OneFS statistics summary API.
// Some of the fields are optional and depend on the protocol in use.
type clientSummaryResult struct {
	//required Performance Metrix

	In_rate        float64 `json:"in"`             // Rate of input (in bytes/second) for an operation since the last time isi statistics collected the data.
	In_avg         float64 `json:"in_avg"`         // Average input (received) bytes for an operation, in bytes.
	In_max         float64 `json:"in_max"`         // Maximum input (received) bytes for an operation, in bytes.
	In_min         float64 `json:"in_min"`         // Minimum input (received) bytes for an operation, in bytes.
	Num_operations float64 `json:"num_operations"` // The number of times an operation has been performed.
	Operation_rate float64 `json:"operation_rate"` // The rate (in ops/second) at which an operation has been performed.
	Out_rate       float64 `json:"out"`            // Rate of output (in bytes/second) for an operation since the last time isi statistics collected the data.
	Out_avg        float64 `json:"out_avg"`        // Average output (sent) bytes for an operation, in bytes.
	Out_max        float64 `json:"out_max"`        // Maximum output (sent) bytes for an operation, in bytes.
	Out_min        float64 `json:"out_min"`        // Minimum output (sent) bytes for an operation, in bytes.
	Time_avg       float64 `json:"time_avg"`       // The average elapsed time (in microseconds) taken to complete an operation.
	Time_max       float64 `json:"time_max"`       // The maximum elapsed time (in microseconds) taken to complete an operation.
	Time_min       float64 `json:"time_min"`       // The minimum elapsed time (in microseconds) taken to complete an operation.

	// regular metadata

	Node        int     `json:"node"`        // The node on which the operation was performed.
	Protocol    *string `json:"protocol"`    // The protocol of the operation.
	Class       *string `json:"class"`       // The class of the operation.
	Remote_addr *string `json:"remote_addr"` // The IP address (in dotted-quad form) of the host sending the operation request.
	Remote_name *string `json:"remote_name"` // The resolved text name of the RemoteAddr, if resolution can be performed.
	Local_addr  *string `json:"local_addr"`  // The IP address (in dotted-quad form) of the host receiving the operation request.
	Local_name  *string `json:"local_name"`  // The resolved text name of the LocalAddr, if resolution can be performed.
	Unixtime    int64   `json:"time"`        // Unix Epoch time in seconds of the request.

	// optional criteria

	User UserInfo `json:"user"` // "User issuing the operation."
}

type UserInfo struct {
	Type *string `json:"type"` // Specifies the type of persona, which must be combined with a name. string of [user, group, wellknown] or NIL
	Name *string `json:"name"` // Specifies the persona name, which must be combined with a type.
	Uid  *string `json:"id"`   // Specifies the serialized form of a persona, which can be 'UID:0', 'USER:name', 'GID:0', 'GROUP:wheel', or 'SID:S-1-1'.
}

// clientSummaryQuery describes the result from calling the client summary endpoint
type clientSummaryQuery struct {
	Clients []clientSummaryResult `json:"client"`
}

const sessionPath = "/session/1/session"
const configPath = "/platform/1/cluster/config"
const clientSummaryPath = "/platform/3/statistics/summary/client"
const csKeyName = "isi.client.stats"

const maxTimeoutSecs = 1800 // clamp retry timeout to 30 minutes

// Set up Client etc.
func (c *Cluster) initialize() error {
	// already initialized?
	if c.client != nil {
		return nil
	}
	if c.Username == "" {
		return fmt.Errorf("username must be set")
	}
	if c.Password == "" {
		return fmt.Errorf("password must be set")
	}
	if c.Hostname == "" {
		return fmt.Errorf("hostname must be set")
	}
	if c.Port == 0 {
		c.Port = 8080
	}
	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	if err != nil {
		return err
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: !c.VerifySSL},
	}
	c.client = &http.Client{
		Transport: tr,
		Jar:       jar,
	}
	c.baseURL = "https://" + c.Hostname + ":" + strconv.Itoa(c.Port)
	return nil
}

// Authenticate to the cluster using the session API endpoint
// store the cookies
func (c *Cluster) Authenticate() error {
	var err error
	var resp *http.Response

	am := struct {
		Username string   `json:"username"`
		Password string   `json:"password"`
		Services []string `json:"services"`
	}{
		Username: c.Username,
		Password: c.Password,
		Services: []string{"platform"},
	}
	b, err := json.Marshal(am)
	if err != nil {
		return err
	}
	u, err := url.Parse(c.baseURL + sessionPath)
	if err != nil {
		return err
	}
	// POST our authentication request to the API
	// This may be our first connection so we'll retry here in the hope that if
	// we can't connect to one node, another may be responsive
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewBuffer(b))
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "application/json")
	retrySecs := 1
	for i := 1; i <= c.maxRetries; i++ {
		resp, err = c.client.Do(req)
		if err == nil {
			break
		}
		log.Warningf("Authentication request failed: %s - retrying in %d seconds", err, retrySecs)
		time.Sleep(time.Duration(retrySecs) * time.Second)
		retrySecs *= 2
		if retrySecs > maxTimeoutSecs {
			retrySecs = maxTimeoutSecs
		}
	}
	if err != nil {
		return fmt.Errorf("max retries exceeded for connect to %s, aborting connection attempt", c.Hostname)
	}
	defer resp.Body.Close()
	// 201(StatusCreated) is success
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("Authenticate: auth failed - %s", resp.Status)
	}
	// parse out time limit so we can reauth when necessary
	dec := json.NewDecoder(resp.Body)
	var ar map[string]interface{}
	err = dec.Decode(&ar)
	if err != nil {
		return fmt.Errorf("Authenticate: unable to parse auth response - %s", err)
	}
	// drain any other output
	io.Copy(io.Discard, resp.Body)
	var timeout int
	ta, ok := ar["timeout_absolute"]
	if ok {
		timeout = int(ta.(float64))
	} else {
		// This shouldn't happen, but just set it to a sane default
		log.Warning("authentication API did not return timeout value, using default")
		timeout = 14400
	}
	if timeout > 60 {
		timeout -= 60 // Give a minute's grace to the reauth timer
	}
	c.reauthTime = time.Now().Add(time.Duration(timeout) * time.Second)

	c.csrfToken = ""
	// Dig out CSRF token so we can set the appropriate header
	for _, cookie := range c.client.Jar.Cookies(u) {
		if cookie.Name == "isicsrf" {
			log.Debugf("Found csrf cookie %v\n", cookie)
			c.csrfToken = cookie.Value
		}
	}
	if c.csrfToken == "" {
		log.Debugf("No CSRF token found for cluster %s, assuming old-style session auth", c.Hostname)
	}

	return nil
}

// GetClusterConfig pulls information from the cluster config API
// endpoint, including the actual cluster name
func (c *Cluster) GetClusterConfig() error {
	var v interface{}
	resp, err := c.restGet(configPath)
	if err != nil {
		return err
	}
	err = json.Unmarshal(resp, &v)
	if err != nil {
		return err
	}
	m := v.(map[string]interface{})
	version := m["onefs_version"]
	r := version.(map[string]interface{})
	release := r["version"]
	rel := release.(string)
	c.OSVersion = rel
	if c.normalize {
		c.ClusterName = strings.ToLower(m["name"].(string)) //Config option
	} else {
		c.ClusterName = m["name"].(string)
	}

	return nil
}

// Connect establishes the initial network connection to the cluster,
// then pulls the cluster config info to get the real cluster name
func (c *Cluster) Connect() error {
	var err error
	if err = c.initialize(); err != nil {
		return err
	}
	if c.AuthType == authtypeSession {
		if err = c.Authenticate(); err != nil {
			return err
		}
	}
	if err = c.GetClusterConfig(); err != nil {
		return err
	}
	return nil
}

// GetCLientStats retrieves client summary statistics and returns array of clientSummaryResult structures.
func (c *Cluster) GetClientStats() ([]clientSummaryResult, error) {
	var results []clientSummaryResult

	basePath := clientSummaryPath + "?degraded=true&nodes=all"
	// Need special case for short last get
	log.Infof("fetching Client Summary stats from cluster %s", c.ClusterName)
	// log.Debugf("cluster %s fetching %s", c.ClusterName, buffer.String())
	resp, err := c.restGet(basePath)
	if err != nil {
		log.Errorf("Attempt to retrieve client summary data for cluster %s, failed - %s", c.ClusterName, err)
		return nil, err
	}
	log.Debugf("client summary response: %s", resp)
	// Parse the result
	results, err = parseClientSummaryResult(resp)
	if err != nil {
		log.Errorf("Unable to parse stat response - %s", err)
		return nil, err
	}

	return results, nil
}

func parseClientSummaryResult(res []byte) ([]clientSummaryResult, error) {
	// XXX need to handle errors response here!
	cSummary := clientSummaryQuery{}
	err := json.Unmarshal(res, &cSummary)
	if err != nil {
		return nil, err
	}
	return cSummary.Clients, nil
}

// helper function
func isConnectionRefused(err error) bool {
	if uerr, ok := err.(*url.Error); ok {
		if nerr, ok := uerr.Err.(*net.OpError); ok {
			if oerr, ok := nerr.Err.(*os.SyscallError); ok {
				if oerr.Err == syscall.ECONNREFUSED {
					return true
				}
			}
		}
	}
	return false
}

// get REST response from the API
func (c *Cluster) restGet(endpoint string) ([]byte, error) {
	var err error
	var resp *http.Response

	if c.AuthType == authtypeSession && time.Now().After(c.reauthTime) {
		log.Infof("re-authenticating to cluster %v based on timer", c.ClusterName)
		if err = c.Authenticate(); err != nil {
			return nil, err
		}
	}

	u, err := url.Parse(c.baseURL + endpoint)
	if err != nil {
		return nil, err
	}
	req, err := c.newGetRequest(u.String())
	if err != nil {
		return nil, err
	}

	retrySecs := 1
	for i := 1; i < c.maxRetries; i++ {
		resp, err = c.client.Do(req)
		if err == nil {
			// We got a valid http response
			if resp.StatusCode == http.StatusOK {
				break
			}
			// check for need to re-authenticate (maybe we are talking to a different node)
			if resp.StatusCode == http.StatusUnauthorized {
				resp.Body.Close()
				if c.AuthType == authtypeBasic {
					return nil, fmt.Errorf("basic authentication for cluster %v failed - check username and password", c.ClusterName)
				}
				log.Noticef("Authentication to cluster %v failed, attempting to re-authenticate", c.ClusterName)
				if err = c.Authenticate(); err != nil {
					return nil, err
				}
				req, err = c.newGetRequest(u.String())
				if err != nil {
					return nil, err
				}
				continue
				// TODO handle repeated auth failures to avoid panic
			}
			resp.Body.Close()
			return nil, fmt.Errorf("Cluster %v returned unexpected HTTP response: %v", c.ClusterName, resp.Status)
		}
		// assert err != nil
		// TODO - consider adding more retryable cases e.g. temporary DNS hiccup
		if !isConnectionRefused(err) {
			return nil, err
		}
		log.Errorf("Connection to %s refused, retrying in %d seconds", c.Hostname, retrySecs)
		time.Sleep(time.Duration(retrySecs) * time.Second)
		retrySecs *= 2
		if retrySecs > maxTimeoutSecs {
			retrySecs = maxTimeoutSecs
		}
	}
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Cluster %v returned unexpected HTTP response: %v", c.ClusterName, resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	return body, err
}

func (c *Cluster) newGetRequest(url string) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", "application/json")
	if c.AuthType == authtypeBasic {
		req.SetBasicAuth(c.AuthInfo.Username, c.AuthInfo.Password)
	}
	if c.csrfToken != "" {
		// Must be newer session-based auth with CSRF protection
		req.Header.Set("X-CSRF-Token", c.csrfToken)
		req.Header.Set("Referer", c.baseURL)
	}
	return req, nil
}
