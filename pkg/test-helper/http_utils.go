package test_helper

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

type Connection struct {
	Name        string `json:"name"`
	ContainerId string `json:"container_id"`
}

func Connections() ([]Connection, error) {
	bodyString, err := httpGet("http://localhost:15672/api/connections/", "guest", "guest")
	if err != nil {
		return nil, err
	}

	var data []Connection
	err = json.Unmarshal([]byte(bodyString), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetConnectionByContainerID(Id string) (*Connection, error) {
	connections, err := Connections()
	if err != nil {
		return nil, err
	}
	for _, conn := range connections {
		if conn.ContainerId == Id {
			return &conn, nil
		}
	}

	return nil, errors.New("connection not found")
}

func DropConnectionContainerID(Id string) error {
	connections, err := Connections()
	if err != nil {
		return err
	}
	connectionToDrop := ""
	for _, conn := range connections {
		if conn.ContainerId == Id {
			connectionToDrop = conn.Name
			break
		}
	}

	if connectionToDrop == "" {
		return errors.New("connection not found")
	}

	err = DropConnection(connectionToDrop, "15672")
	if err != nil {
		return err
	}
	return nil
}

func DropConnection(name string, port string) error {
	_, err := httpDelete("http://localhost:"+port+"/api/connections/"+name, "guest", "guest")
	if err != nil {
		return err
	}

	return nil
}

func CreateVirtualHost(name string) error {
	_, err := httpPut("http://localhost:15672/api/vhosts/"+name, "guest", "guest")
	if err != nil {
		return err
	}
	return nil
}

func DeleteVirtualHost(name string) error {
	_, err := httpDelete("http://localhost:15672/api/vhosts/"+name, "guest", "guest")
	if err != nil {
		return err
	}
	return nil
}

func httpGet(url, username, password string) (string, error) {
	return baseCall(url, username, password, "GET")
}

func httpDelete(url, username, password string) (string, error) {
	return baseCall(url, username, password, "DELETE")
}

func httpPut(url, username, password string) (string, error) {
	return baseCall(url, username, password, "PUT")
}

func baseCall(url, username, password string, method string) (string, error) {
	var client http.Client
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(username, password)

	resp, err3 := client.Do(req)

	if err3 != nil {
		return "", err3
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 { // OK
		bodyBytes, err2 := io.ReadAll(resp.Body)
		if err2 != nil {
			return "", err2
		}
		return string(bodyBytes), nil
	}

	if resp.StatusCode == 201 {
		// Created! it is ok
		return "", nil
	}

	if resp.StatusCode == 204 { // No Content
		return "", nil
	}

	return "", errors.New(strconv.Itoa(resp.StatusCode))

}
