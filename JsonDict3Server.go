package main
//concurrency map - https://blog.golang.org/go-maps-in-action
import (
	"fmt"
	"net/rpc"
	"net/rpc/jsonrpc"
	"encoding/json"
	"os"
	"net"
	"bufio"
    "log"
    "strings"
    "io/ioutil"
    "crypto/sha1"
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

type DIC3 int

var dict3 map[DKey]map[string]interface{}
var configMap map[string]interface{}
var protocol string
var ipAdd string
var dict3File string
var methods []interface{}
var port string
var confLoc string
var successor string
var shakey string
var chordid int
var fingertable map[int]string

//server shutdown
func (t *DIC3) Shutdown(dKey *DKey, reply *int) error{
	fmt.Println("shuting server down!!!!!!")
	*reply = 9
	persistDict3()
	os.Exit(0)
	return nil
}
//Lookup returns value stored for given key and relation
func (t *DIC3) Lookup(req *Request, reply *Response) error {
	fmt.Println("in Lookup : ", req.KeyRel.KeyA, req.KeyRel.RelA, req.Id)
	val := dict3[req.KeyRel]

        if val != nil {

                reply.ID = req.Id
           	reply.Tripair.Key = req.KeyRel.KeyA
           	reply.Tripair.Rel = req.KeyRel.RelA
           	reply.Tripair.Val = val
           	reply.Done = true
           	reply.Err = nil
           	return nil
        } else {

           krhash  := getKeyRelHash(req.KeyRel.KeyA,req.KeyRel.RelA)
           service := findSuccessor(krhash)
           client, err := jsonrpc.Dial(protocol, service)
           if err != nil {
                log.Fatal("dialing:", err)
           }
           var reply2 Response
           RpcCall := client.Go("DIC3.Lookup", req, &reply2,nil)
           replyCall := <-RpcCall.Done
           if replyCall != nil {
           }

                reply.ID = reply2.ID
                reply.Tripair.Key = reply2.Tripair.Key
                reply.Tripair.Rel = reply2.Tripair.Rel
                reply.Tripair.Val = reply2.Tripair.Val
                reply.Done = reply2.Done
                reply.Err = reply2.Err

           fmt.Println(reply)
           client.Close()
        }
        fmt.Println("get back to main")
        return nil

}
//Insert a given triplet in DICT3
func (t *DIC3) Insert(triplet *Request, reply *Response) error {
	fmt.Println("in Insert : ", triplet.KeyRel.KeyA, triplet.KeyRel.RelA, triplet.Val, triplet.Id)

	dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}] = triplet.Val
	_, ok := dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}]
	reply.ID = triplet.Id
	reply.Done = ok
	reply.Err = nil
	return nil
}

//InsertOrUpdate given triplet in DICT3
func (t *DIC3) InsertOrUpdate(triplet *Request, reply *Response) error {
	fmt.Println("in InsertOrUpdate : ", triplet.KeyRel.KeyA, triplet.KeyRel.RelA, triplet.Val, triplet.Id)
	keyRel := DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}
	_, ok := dict3[keyRel]
	if !ok {
		//Insert
		fmt.Println("Inserting.....")
		dict3[keyRel] = triplet.Val
	}else{
		//Update
		fmt.Println("Updating.....")
		delete(dict3, keyRel)
		dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}] = triplet.Val
	}
	reply.ID = triplet.Id
	reply.Done = ok
	reply.Err = nil
	return nil
}

//Delete from DICT3
func (t *DIC3) Delete(req *Request, reply *Response) error {
	fmt.Println("in Delete : ", req.Id, req.KeyRel.KeyA, req.KeyRel.RelA)
	delete(dict3, req.KeyRel)
	fmt.Println("after delete DICT3 ", dict3)
	reply.ID = req.Id
	reply.Done = true
	reply.Err = nil
	return nil
}

//list keys in DICT3
func (t *DIC3) Listkeys(req *Request, reply *ListResponse) error {
	fmt.Println("in ListKeys:: Request ID : ", req.Id)
	keys := make([]string, 0, len(dict3))
    for k := range dict3 {
    	found := checkIfPresent(k.KeyA, keys)
    	if !found{
        	keys = append(keys, k.KeyA)
        	}
    }
	reply.Id = req.Id
	reply.List = keys
	reply.Err = nil
	return nil
}

//list key-relation pairs in DICT3
func (t *DIC3) ListIDs(req *Request, reply *ListResponse) error {
	fmt.Println("in ListIDs:: Request ID : ", req.Id)
	keys := make([]string, 0, len(dict3))
    for k := range dict3 {
        keys = append(keys, "[",k.KeyA,",", k.RelA, "]")
    }
	reply.Id = req.Id
	reply.List = keys
	reply.Err = nil
	return nil
}

func main() {

	loadConfig()
        chordid = getChordId(port,ipAdd)
	loadDict3()
        fingertable = make(map[int]string)
        sid := getChordId(successor,ipAdd)
        ls := []string{ipAdd,successor}
        fingertable[sid] = strings.Join(ls,"")

        //makeChordRing()

	dic3 := new(DIC3)
	rpc.Register(dic3)

	tcpAddr, err := net.ResolveTCPAddr(protocol, port)
	checkError(err)
	fmt.Println("Server started........")
	listener, err := net.ListenTCP(protocol, tcpAddr)
	checkError(err)

        getRelHash("Neha")
        getKeyHash("Tilak2")
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		jsonrpc.ServeConn(conn)
	}

}
//load config in configMap
func loadConfig() error{
	configMap = make(map[string]interface{})
	fmt.Println("Reading ", os.Args[1])
	dat, err := ioutil.ReadFile(os.Args[1])
    checkError(err)
    //fmt.Print(dat)

	if err := json.Unmarshal(dat, &configMap); err != nil {
        log.Fatal("Error in loading config ", err)
    }
	protocol = configMap["protocol"].(string)
	ipAdd = configMap["ipAddress"].(string)
	port = configMap["port"].(string)
        successor = configMap["successor"].(string)
	persiStorage := configMap["persistentStorageContainer"].(map[string]interface{})
	dict3File = persiStorage["file"].(string)
	methods = configMap["methods"].([]interface{})
	fmt.Println("Methods exposed by server: ", methods)
	return nil
}
//load DICT3 in memory from persistent storage
func loadDict3() error{
	dict3 = make(map[DKey]map[string]interface{})

	file, err := os.Open(dict3File)
	if err != nil {
    	log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
    	arr := strings.Split(scanner.Text(), "=")
    	if len(arr) == 3{
    		key_rel := DKey{arr[0], arr[1]}
    		b :=[]byte(arr[2])
			var f map[string]interface{}
			err := json.Unmarshal(b, &f)
			if err != nil{
				log.Fatal(err)
			}
			dict3[key_rel] = f
		}
	}
	fmt.Println(dict3)
	if err := scanner.Err(); err != nil {
    	log.Fatal(err)
	}
	return nil
}

func setFingers() {

}
func persistDict3()error{
	 // For more granular writes, open a file for writing.
    f, err := os.Create(dict3File)
    checkError(err)
    defer f.Close()

	for k, v := range dict3{
		b, err := json.Marshal(v)
		val := string(b[:])
		s := []string{k.KeyA, "=", k.RelA,"=", val,"\n"}
		_, err = f.WriteString(strings.Join(s, ""))
		checkError(err)
		f.Sync()
	}
	return nil
}

func getKeyRelHash(k string, r string) int {
           h1 := sha1.New()
           h1.Write([]byte(k))
           b1 := h1.Sum(nil)
           data1 := b1[0]
           id1 := data1 % 8
           id1 = id1 * 4

           h2 := sha1.New()
           h2.Write([]byte(r))
           b2 := h2.Sum(nil)
           data2 := b2[0]
           id2 := data2 % 4
           retid := int(id1 + id2)
           fmt.Println("Hash for key-rel=" , retid)
           return retid
}

func getKeyHash(k string) [4]int {
           h1 := sha1.New()
           h1.Write([]byte(k))
           b1 := h1.Sum(nil)
           data1 := b1[0]
           id1 := data1 % 8
           id1 = id1 * 4
           idint := int(id1)
           var nodelist [4]int

           nodelist[0] = (idint + 0)
           nodelist[1] = (idint + 1)
           nodelist[2] = (idint + 2)
           nodelist[3] = (idint + 3)

           fmt.Println("Nodelist for given key" , nodelist)
           return nodelist
}

func getRelHash(r string) [8]int {
           h1 := sha1.New()
           h1.Write([]byte(r))
           b1 := h1.Sum(nil)
           data1 := b1[0]
           id1 := data1 % 4
           idint := int(id1)
           var nodelist [8]int

           for k := 0; k<8 ; k++ {
               nodelist[k] = (k*4) + idint
           }

           fmt.Println("Nodelist for given relation" , nodelist)
           return nodelist
}

func getChordId(po string, ip string) int{
           addr := []string{po,ip}
           idaddr := strings.Join(addr,"")

           h1 := sha1.New()
           h1.Write([]byte(idaddr))
           b1 := h1.Sum(nil)

           bb1 := int(b1[3]) + int(b1[2])*10 + int(b1[8])*20 + int(b1[12])*30
           cid := bb1 % 32

           fmt.Println("Chord id = ", cid)
           return cid
}

func findSuccessor(nid int) string {

           val := fingertable[nid]
           if val != "" {
              return val;
           } else {
             for k := 1; k< 32; k++ {
                 nextone := (nid + k) % 32
                 val1 := fingertable[nextone]
                 if val1 != "" {
                    return val1
                 }
             }
           }
           return fingertable[0]
}

func checkIfPresent(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
