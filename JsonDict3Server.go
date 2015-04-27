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
    "math"
    "time"
    "reflect"
)

type DKey struct{
        KeyA, RelA string
}
type Request struct{
        KeyRel DKey
        Val map[string]interface{}
        Permission string
}
type Response struct{
        Tripair Triplet
        Done bool
        ID int
        Err error
}

type ListRequest struct {
        SourceId int
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

type NodeInfo struct {
        Chordid int
        Address string
}

type DIC3 struct{}

var dict3 map[DKey]map[string]interface{}
var configMap map[string]interface{}
var protocol string
var ipAdd string
var dict3File string
var methods []interface{}
var port string
var confLoc string
var successor NodeInfo
var predecessor  NodeInfo
var shakey string
var chordid int
var selfaddr string
var fingertable map[int]NodeInfo
var purgeChan chan struct{}
var purgeInterval int

//server shutdown
func (t *DIC3) Shutdown(dKey *DKey, reply *int) error{
        fmt.Println("shuting server down!!!!!!")
        *reply = 9
        persistDict3()
        close(purgeChan)
        os.Exit(0)
        return nil
}
//Lookup returns value stored for given key and relation
func (t *DIC3) Lookup(req *Request, reply *Response) error {
        fmt.Println("in Lookup : ", req.KeyRel.KeyA, req.KeyRel.RelA)
        val := dict3[req.KeyRel]

        if val != nil {

                val["accessed"] = time.Now()
                reply.Tripair.Key = req.KeyRel.KeyA
                reply.Tripair.Rel = req.KeyRel.RelA
                reply.Tripair.Val = val
                reply.Done = true
                reply.Err = nil
                return nil
        } else {

           krhash  := getKeyRelHash(req.KeyRel.KeyA,req.KeyRel.RelA)
           if krhash == chordid {
              reply.Done = true
              reply.Err = nil
              return nil
           }
           //service := findSuccessor(krhash)
           //if belongsto(krhash,chordid,successor.Chordid) {
              service := successor.Address
           //}
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

//Partial Key Lookup
func (t *DIC3) Partial_Key_Lookup(req *Request, reply *ListResponse) error {
        fmt.Println("in Partial_Key_Lookup : ", req.KeyRel.KeyA)

        result := make([]Triplet, 1)
        for k, v := range dict3{
                if(strings.EqualFold(k.KeyA, req.KeyRel.KeyA)){
                        result = append(result, Triplet{k.KeyA, k.RelA, v})
                        v["accessed"] = time.Now()      //should it be changed?
                }
        }
        reply.List = result
        reply.Err = nil
        return nil
}

//Partial Relation Lookup
func (t *DIC3) Partial_Rel_Lookup(req *Request, reply *ListResponse) error {
        fmt.Println("in Partial_Rel_Lookup : ", req.KeyRel.RelA)
        result := make([]Triplet, 1)
        for k, v := range dict3{
                if(strings.EqualFold(k.RelA, req.KeyRel.RelA)){
                        result = append(result, Triplet{k.KeyA, k.RelA, v})
                        v["accessed"] = time.Now()      //should it be changed?
                }
        }
        reply.List = result
        reply.Err = nil
        return nil
}

//Insert a given triplet in DICT3
func (t *DIC3) Insert(triplet *Request, reply *Response) error {
        fmt.Println("in Insert : ", triplet.KeyRel.KeyA, triplet.KeyRel.RelA,
triplet.Val)

        hashid := getKeyRelHash(triplet.KeyRel.KeyA, triplet.KeyRel.RelA)
        if belongsto(hashid, predecessor.Chordid, chordid) == true {
                //dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}] =
                //triplet.Val
                insertTripletToDict3(triplet.KeyRel, triplet.Val,
triplet.Permission)
                _, ok := dict3[DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}]
                reply.Done = ok
                reply.Err = nil
                return nil
       }
       np := nearestPredecessor(hashid)
       client, err := jsonrpc.Dial(protocol, np.Address)
       if err != nil {
            log.Fatal("dialing:", err)
       }
       var reply2 Response
       RpcCall := client.Go("DIC3.Insert", triplet, &reply2,nil)
       replyCall := <-RpcCall.Done
       if replyCall != nil {
       }

       reply.Done = reply2.Done
       reply.Err = reply2.Err
       fmt.Println(reply)
       client.Close()
       return nil
}

func insertTripletToDict3(dkey DKey, val map[string]interface{}, perm string){
        valJson := make(map[string]interface{})
        valJson["content"] = val
        valJson["size"] = reflect.TypeOf(val).Size()
        valJson["created"] = time.Now()
        valJson["accessed"] = time.Now()
        valJson["modified"] = time.Now()
        valJson["permission"] = perm

        dict3[DKey{dkey.KeyA, dkey.RelA}] = valJson

}

//InsertOrUpdate given triplet in DICT3
func (t *DIC3) InsertOrUpdate(triplet *Request, reply *Response) error {
        fmt.Println("in InsertOrUpdate : ", triplet.KeyRel.KeyA,
triplet.KeyRel.RelA, triplet.Val)

        hashid := getKeyRelHash(triplet.KeyRel.KeyA, triplet.KeyRel.RelA)
        if belongsto(hashid, predecessor.Chordid, chordid) == true {
           keyRel := DKey{triplet.KeyRel.KeyA, triplet.KeyRel.RelA}
           v, ok := dict3[keyRel]
           if !ok {
                //Insert
                fmt.Println("Inserting.....")
                //dict3[keyRel] = triplet.Val
                insertTripletToDict3(triplet.KeyRel, triplet.Val,
triplet.Permission)
           }else{
                //Update
                fmt.Println("Updating.....")
                access := v["permission"].(string)
                if  (strings.EqualFold("RW", access)){
                        v["content"] = triplet.Val
                        v["size"] = reflect.TypeOf(triplet.Val).Size()
                        v["modified"] = time.Now()
                        _, ok = dict3[DKey{triplet.KeyRel.KeyA,
triplet.KeyRel.RelA}]
                } else {
                        fmt.Println("No RW access")
                }
           }

        reply.Done = ok
        reply.Err = nil
        return nil
        }
       np := nearestPredecessor(hashid)
       client, err := jsonrpc.Dial(protocol, np.Address)
       if err != nil {
            log.Fatal("dialing:", err)
       }
       var reply2 Response
       RpcCall := client.Go("DIC3.InsertOrUpdate", triplet, &reply2,nil)
       replyCall := <-RpcCall.Done
       if replyCall != nil {
       }

       reply.Done = reply2.Done
       reply.Err = reply2.Err
       fmt.Println(reply)
       client.Close()
       return nil

}

//Delete from DICT3
func (t *DIC3) Delete(req *Request, reply *Response) error {
        fmt.Println("in Delete : ", req.KeyRel.KeyA, req.KeyRel.RelA)
        delete(dict3, req.KeyRel)
        fmt.Println("after delete DICT3 ", dict3)
        reply.Done = true
        reply.Err = nil
        return nil

        v, ok := dict3[DKey{req.KeyRel.KeyA, req.KeyRel.RelA}]
        if ok {
            access := v["permission"].(string)
                if(strings.EqualFold("RW", access)){
                        delete(dict3, req.KeyRel)
                        fmt.Println("after delete DICT3 ", dict3)

                }else{
                        fmt.Println("No RW access!!")
                }
        reply.Done = true
        reply.Err = nil
        return nil
        }

       hashid := getKeyRelHash(req.KeyRel.KeyA, req.KeyRel.RelA)
       np := nearestPredecessor(hashid)
       client, err := jsonrpc.Dial(protocol, np.Address)
       if err != nil {
            log.Fatal("dialing:", err)
       }
       var reply2 Response
       RpcCall := client.Go("DIC3.Delete", req, &reply2,nil)
       replyCall := <-RpcCall.Done
       if replyCall != nil {
       }

       reply.Done = reply2.Done
       reply.Err = reply2.Err
       fmt.Println(reply)
       client.Close()
       return nil

}

//list keys in DICT3
func (t *DIC3) Listkeys(req *ListRequest, reply *ListResponse) error {
        var req2 ListRequest
        if req.SourceId == -1 {
           req2.SourceId = chordid
        }
        if successor.Chordid == req.SourceId {

            keys := make([]string, 0, len(dict3))
            for k := range dict3 {
                found := checkIfPresent(k.KeyA, keys)
                if !found{
                      keys = append(keys, k.KeyA)
                }
            }
        reply.List = keys
        reply.Err = nil
        return nil
        } else {
            client, err := jsonrpc.Dial(protocol, successor.Address)
            if err != nil {
                log.Fatal("dialing:", err)
            }
            var reply2 ListResponse
            RpcCall := client.Go("DIC3.ListKeys", req2, &reply2,nil)
            replyCall := <-RpcCall.Done
            if replyCall != nil {
            }
            tempvar := reply2.List.([]string)
            fmt.Println(tempvar)
            keys := make([]string, 0, (len(dict3) + len(tempvar)))
            for k := range dict3 {
                found := checkIfPresent(k.KeyA, keys)
                if !found{
                      keys = append(keys, k.KeyA)
                }
            }
            for j:= range tempvar {
                found := checkIfPresent(tempvar[j], keys)
                if !found{
                      keys = append(keys, tempvar[j])
                }
            }
            reply.List = keys
            reply.Err = nil
            return nil
          }
          return nil
}

//list key-relation pairs in DICT3
func (t *DIC3) ListIDs(req *ListRequest, reply *ListResponse) error {
        var req2 ListRequest
        if req.SourceId == -1 {
           req2.SourceId = chordid
        }
        if successor.Chordid == req.SourceId {

            keys := make([]string, 0, len(dict3))
            for k := range dict3 {
                keys = append(keys, "[",k.KeyA,",",k.RelA,"]")
            }
        reply.List = keys
        reply.Err = nil
        return nil
        } else {
            client, err := jsonrpc.Dial(protocol, successor.Address)
            if err != nil {
                log.Fatal("dialing:", err)
            }
            var reply2 ListResponse
            RpcCall := client.Go("DIC3.ListIDs", req2, &reply2,nil)
            replyCall := <-RpcCall.Done
            if replyCall != nil {
            }
            tempvar := reply2.List.([]string)
            fmt.Println(tempvar)
            keys := make([]string, 0, (len(dict3) + len(tempvar)))
            for k := range dict3 {
                found := checkIfPresent(k.KeyA, keys)
                if !found{
                      keys = append(keys, k.KeyA)
                }
            }
            for j:= range tempvar {
                found := checkIfPresent(tempvar[j], keys)
                if !found{
                      keys = append(keys, tempvar[j])
                }
            }
            reply.List = keys
            reply.Err = nil
            return nil
          }
          return nil
}

func (t *DIC3) Notify(request NodeInfo, reply *NodeInfo) error {
        if predecessor.Chordid == -1 ||
belongsto(request.Chordid,predecessor.Chordid,chordid) == true {
           predecessor.Chordid = request.Chordid
           predecessor.Address = request.Address
           return nil
        }
        return nil
}

func (t *DIC3) FindSuccessor(request NodeInfo, reply *NodeInfo) error {
        fmt.Println("Control in FindSuccessor method")
        fmt.Println("Requesting node", request)
        if belongsto(request.Chordid, chordid, successor.Chordid) == true {
                reply.Chordid = successor.Chordid
                reply.Address = successor.Address
                prevSuc := successor
                successor.Chordid = request.Chordid
                successor.Address = request.Address
                fingertable[1] = successor
                fmt.Println("Updated successor as ",successor)

                // Notify previous successor
                if prevSuc.Chordid != chordid {
                   client1, err := jsonrpc.Dial(protocol, prevSuc.Address)
                   if err != nil {
                        log.Fatal("dialing:", err)
                   }
                   var replynull NodeInfo

                   RpcCall := client1.Go("DIC3.FindNotify", request,
&replynull,nil)
                   replyCall := <-RpcCall.Done
                   if replyCall != nil {
                   }
                   client1.Close()
                }

                return nil
        } else {
                np := nearestPredecessor(request.Chordid)
                client, err := jsonrpc.Dial(protocol, np.Address)
                if err != nil {
                     log.Fatal("dialing:", err)
                }
                var reply2 NodeInfo
                RpcCall := client.Go("DIC3.FindSuccessor", request, &reply2,nil)
                replyCall := <-RpcCall.Done
                if replyCall != nil {
                }
                client.Close()
                    reply.Address = reply2.Address
                    reply.Chordid = reply2.Chordid
                    return nil
        }
        return nil
}

func belongsto (num int, start int, end int) bool {
        diff1 := end - start
        if diff1 <= 0 {
           diff1 = diff1 + 32
        }
        diff2 := num - start
        if diff2 <= 0 {
           diff2 = diff2 + 32
        }
        if diff1 > diff2 {
           return true
        }
        return false
}

//background process for purging unused triplets
func purgeUnusedTriplets(){
        //not prefered because it just considers gap between execution, not
        //schedule
        //time.AfterFunc(time.Second*time.Duration(5), purgeUnusedTriplets)

        purgeTicker := time.NewTicker(time.Duration(purgeInterval) *
time.Second)
        purgeChan = make(chan struct{})
        go func() {
                for {
                        select {
                        case <-purgeTicker.C:
                                fmt.Println("Purging Unused triplets ",
time.Now())
                                for k, v := range dict3{
                                        access := v["permission"].(string)
                                        if(strings.EqualFold("RW", access)){
                                                accessed :=
v["accessed"].(string)
                                                access_time, err :=
time.Parse("2006-01-02 15:04", accessed)
                                                checkError(err)
                                                duration :=
time.Since(access_time)
                                                if(duration >
time.Duration(purgeInterval)){
                                                        //delete triplet
                                                        delete(dict3, k)
                                                }
                                        }
                                }
                        case <-purgeChan:
                                purgeTicker.Stop()
                                fmt.Println("Stopped the purgeTicker!")
                                return
                        }
                }
        }()
}

func main() {

        loadConfig()
        //chordid = getChordId(port,ipAdd)
        loadDict3()
        fingertable = make(map[int]NodeInfo)
        //sid := getChordId(successor,ipAdd)
        //ls := []string{ipAdd,successor}
        //fingertable[sid] = strings.Join(ls,"")

        createChordRing()

//        knownip := "localhost:1234"
//        joinChordRing(knownip)

        dic3 := new(DIC3)
        rpc.Register(dic3)

        tcpAddr, err := net.ResolveTCPAddr(protocol, port)
        checkError(err)
        fmt.Println("Server started........")
        listener, err := net.ListenTCP(protocol, tcpAddr)
        checkError(err)

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
        addr := []string{ipAdd,port}
        selfaddr = strings.Join(addr,"")
        if selfaddr == "" {
          fmt.Println("Could not initialize selfaddr")
        }
        chordid = getChordId(port,ipAdd)
         persiStorage :=
configMap["persistentStorageContainer"].(map[string]interface{})
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

func createChordRing() {
           predecessor.Chordid = -1
           successor.Address = selfaddr
           successor.Chordid = chordid
           predecessor.Address = successor.Address
           predecessor.Chordid = successor.Chordid
           fingertable[1] = NodeInfo{chordid,selfaddr}
           fmt.Println("createChordRing: successor =", successor.Address)
}

func joinChordRing(naddr string) error {
           predecessor.Chordid = -1
           service := naddr
           client, err := jsonrpc.Dial(protocol, service)
           if err != nil {
                log.Fatal("dialing:", err)
           }
           var reply NodeInfo
           request := NodeInfo{chordid,selfaddr}
           RpcCall := client.Go("DIC3.FindSuccessor", request, &reply,nil)
           replyCall := <-RpcCall.Done
           if replyCall != nil {
           }
           successor.Address = reply.Address
           successor.Chordid = reply.Chordid
           fingertable[1] = NodeInfo{reply.Chordid,reply.Address}
           fmt.Println("Joined chord ring")
           fmt.Println("joinChordRing: successor =", successor.Address)
           client.Close()
           return nil

}

func findSuccessor(nid int) string {

           var diff int
           if nid > chordid {
              diff = nid - chordid
           } else {
              diff = nid + (32 - chordid)
           }

           ind := int(math.Log2(float64(diff)))
           return fingertable[(ind+1)].Address

}

func nearestPredecessor(nid int) NodeInfo {
       flen := len(fingertable)
       for j:= flen; j > 0 ; j-- {
           if belongsto(fingertable[j].Chordid , chordid, nid) {
              return fingertable[j]
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
