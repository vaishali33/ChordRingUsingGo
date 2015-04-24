package main

import (
	"net/rpc/jsonrpc"
	"fmt"
	"log"
	"os"
	"encoding/json"
	"io/ioutil"
	"strings"
	"bufio"
)

type DKey struct{
	KeyA, RelA string
}
type Request struct{
	KeyRel DKey
	Val map[string]interface{}
	Id int
}
type Response struct{
	Tripair Triplet
	Done bool
	ID int
	Err error
}

type Triplet struct{
	Key, Rel string
	Val map[string]interface{}
}

type ListResponse struct{
	List interface{}
	Id int
	Err error
}

var configMap map[string]interface{}
var protocol string
var ipAdd string
var dict3File string
var methods []interface{}
var port string

func main() {

	loadConfig()

	//get Connection to server
	ls := []string{ipAdd,port}
	service := strings.Join(ls,"")
	client, err := jsonrpc.Dial(protocol, service)
	if err != nil {
		log.Fatal("dialing:", err)
	}

	//read instructions to execute from input file
	file, err := os.Open(os.Args[2])
    checkError(err)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		command := strings.Split(scanner.Text(), "(")
    	method, par := command[0], command[1]
    	par = par[:len(par)-1]

    	switch method {

    		case "lookup":{
    			// Lookup request
    			params := strings.Split(par, ",")
				dKey := DKey{params[0], params[1]}
				req := Request{dKey, nil, 96}
				var reply Response
				//err = client.Call("DIC3.Lookup", req, &reply)
				RpcCall := client.Go("DIC3.Lookup", req, &reply,nil)
                                replyCall := <-RpcCall.Done
                                fmt.Println(replyCall)
                                //fmt.Println(divCall)
                                //if replyCall == nil {
                                //        log.Fatal("Async Call error:", err)
                                //}
				if reply.Tripair.Val != nil {
					fmt.Println("lookup:: ", reply)
				}else{
					fmt.Println("lookup:: No such triplet for key relation : ", dKey)
				}

    		}

    		case "insert":{
    			// Insert request
    			params := strings.Split(par, ",")
				var v1 map[string]interface{}
				b :=[]byte(strings.Join(params[2:],","))
				err := json.Unmarshal(b, &v1)
				if err != nil{
					log.Fatal(err)
				}

				tri := Request{DKey{params[0], params[1]}, v1, 0}
				var ok Response
				err = client.Call("DIC3.Insert", tri, &ok)
				if err != nil {
					log.Fatal("DIC3 error:", err)
				}
				fmt.Println("insert successful:: ", ok.Done)
    		}

    		case "insertOrUpdate":{
    			// InsertOrUpdate request
				params := strings.Split(par, ",")
				var v1 map[string]interface{}
				b :=[]byte(strings.Join(params[2:],","))
				err := json.Unmarshal(b, &v1)
				if err != nil{
					log.Fatal(err)
				}
				tri := Request{DKey{params[0], params[1]}, v1, 0}
				var ok Response
				err = client.Call("DIC3.InsertOrUpdate", tri, &ok)
				if err != nil {
					log.Fatal("DIC3 error:", err)
				}
				fmt.Println("insert or update executed")
    		}

    		case "delete":{
    			// Delete request
    			params := strings.Split(par, ",")
				dKey := DKey{params[0], params[1]}
				req := Request{dKey, nil, 5}
				var ok Response
				err := client.Call("DIC3.Delete", req, &ok)
				if err != nil {
					log.Fatal("DIC3 error:", err)
				}
				fmt.Println("delete executed")
    		}

    		case "listKeys":{
    			//list keys in DICT3
				var respk ListResponse
				req := Request{DKey{}, nil, 691}
				err := client.Call("DIC3.Listkeys", req, &respk)
				if err != nil {
					log.Fatal("DIC3 error:", err)
				}
				fmt.Println("list keys:: ", respk)
    		}

    		case "listIDs":{
    			//list key-relation pairs in DICT3
				var resp ListResponse
				req := Request{DKey{}, nil, 699}
				err := client.Call("DIC3.ListIDs", req, &resp)
				if err != nil {
					log.Fatal("DIC3 error:", err)
				}
				fmt.Println("list ids:: ", resp)
    		}

    		case "shutdown":{
    			//shutdown request
				var out int
				client.Call("DIC3.Shutdown", nil, &out)
				fmt.Println("shutdown executed..")
    		}
    		default:
    			fmt.Println("No such method named - ", method)
    	}
	}
    file.Close()
}

//load config in configMap
func loadConfig() error{
	configMap = make(map[string]interface{})
	fmt.Println("Reading ", os.Args[1])
	dat, err := ioutil.ReadFile(os.Args[1])
    checkError(err)
    //fmt.Print(dat)

	if err = json.Unmarshal(dat, &configMap); err != nil {
        log.Fatal("Error in loading config ", err)
    }
	protocol = configMap["protocol"].(string)
	ipAdd = configMap["ipAddress"].(string)
	port = configMap["port"].(string)

	methods = configMap["methods"].([]interface{})
	fmt.Println("Methods available to client: ", methods)
	return nil
}
func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
