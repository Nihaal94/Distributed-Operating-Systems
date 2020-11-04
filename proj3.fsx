#r "nuget: Akka.FSharp"
#time "on"
#r "nuget: Akka.TestKit" 

open System
open System.Threading
open Akka.Actor
open Akka.FSharp

type Message =
| Create of string * int
| Join of string * int
| UpdateRouteTable of String[]
| Route of String * String * int
| Done


let b = 4.0 // find 2^b
let mutable col = (2.0**b) |> int
let mutable actorsMap : Map<String, IActorRef> = Map.empty
let mutable actorHops: Map<String, Double list> = Map.empty

let insertElement (routeTable: string[,]) (prefixLength:int) (key:string) (j:int) =
   let routeRow = prefixLength
   let routeCol = Int32.Parse(key.[prefixLength].ToString(), Globalization.NumberStyles.HexNumber)
   if not (isNull routeTable.[routeRow, routeCol]) then
      let var = routeTable.[routeRow, routeCol]
      let keyRef = actorsMap.Item var
      if not (isNull keyRef) then 
         keyRef <! Join(key, j)
    
   else
      routeTable.[routeRow, routeCol] <- key

let copyRow i (arr:'T[,]) = arr.[i..i, *] 
                           |> Seq.cast<'T> 
                           |> Seq.toArray 

// Use "byref<int>" to update mutable k
let updateTable (routeTable: string[,]) (prefixLength:int) (key:string) (j:byref<int>) (nodeId:string) = 
   let mutable commonRow: string[] = Array.zeroCreate 0          
               
   while j <= prefixLength do 
      commonRow <- copyRow j routeTable  // copy the kth row to common row
      // printfn "cr: %A for j: %i" commonRow j    
      let commonIndex = Int32.Parse(nodeId.[prefixLength].ToString(), Globalization.NumberStyles.HexNumber) 
      commonRow.[commonIndex] <- nodeId
      let keyRef = actorsMap.Item key   // lookup the actor ref of the given key
      if not (isNull keyRef) then 
         keyRef <! UpdateRouteTable(commonRow)         
      j <- j + 1  // Use "byref<int>"

let checkKey (src:string) (hop:int) =
   if actorHops.ContainsKey(src) then
      let keyList = actorHops.Item src
      let total = keyList.[1]
      let avgHops = keyList.[0]
      let value = [((avgHops*total) + (hop |> double))/ (total + 1.0); total + 1.0]
      actorHops <- actorHops.Add(src, value)
   else
      let value = [hop |> double; 1.0]
      actorHops <- actorHops.Add(src, value)

let checkKeyInLeafSet (key:string) (src:string) (hop:int) =
  let actor = actorsMap.Item(key)
  actor <! Route(key, src, hop + 1)

let routeMsg (key:string) (nodeId:string) (prefixLength:byref<int>)(routeTable:string[,])(src:string)(hop:int)=
     let mutable i = 0
     while key.[i] = nodeId.[i] do
         i<- i+1
     prefixLength <- i
     let mutable routeRow = prefixLength
     let mutable routeCol = Int32.Parse(key.[prefixLength].ToString(), Globalization.NumberStyles.HexNumber)
     if isNull routeTable.[routeRow, routeCol] then
         routeCol <- 0

     actorsMap.Item(routeTable.[routeRow, routeCol])  <! Route(key, src, hop + 1)

let routeRequests (numRequests:int) (listOfNodes:string list)= 
   let mutable n = 1
   let mutable count = 0
   let mutable destId = ""
   while n <= numRequests do
       for srcId in listOfNodes do
           count <- count + 1
           destId <- srcId
           while destId = srcId do
               destId <-  listOfNodes.[Random().Next listOfNodes.Length]
           let srcRef = actorsMap.Item srcId
           srcRef <! Route(destId, srcId, 0)
           Thread.Sleep 5

       printfn "Each actor has completed %i requests" n
       n <- n + 1 


// actor defn
let nodeActor (mailBox:Actor<_>) =
   let mutable nodeId = ""
   let mutable routeTable: string[,] = Array2D.create 0 0 "0" 
   let mutable leafSet : Set<String> = Set.empty
   let mutable rows = 0
   let mutable prefixLength = 0 
   let mutable presentRow = 0 
   let mutable (commonRow: string[]) = Array.zeroCreate 0 // Array.create 0 "0" 
   // https://docs.microsoft.com/en-us/dotnet/fsharp/language-reference/arrays

   
   let rec loop() = actor {
      let! message = mailBox.Receive()
      match message with
         | Create(id, len) -> // Create is sending id and log-value(idLen)
            nodeId <- id  
            // printfn "node id init to %s -------------------------------" nodeId
            rows <- len   // number of rows is equal to length of the nodeId
            let id = Int32.Parse(nodeId, Globalization.NumberStyles.HexNumber) // convert the nodeId(str) to int
            routeTable <- Array2D.zeroCreate rows col   // create routing table with rows-idlen and col- 2 power b
            // Create LeafSet: contains L/2 smaller ids on left and L/2 greater ids on right side
            let mutable l = id

            if l = 0 then 
               for i in 1..col do
                  leafSet <- leafSet.Add((i + 1).ToString("X"))
            if l <> 0 && l < (col/2) then  
               for i in 0..(col) do
                  if (i <> l) then
                     leafSet <- leafSet.Add((i).ToString("X"))

            if l <> 0 && l >= (col/2) then
               for i = (l-1) downto (l-(col/2)) do
                  leafSet <- leafSet.Add((i).ToString("X"))
               for i in (l+1)..(l + (col/2)) do
                  leafSet <- leafSet.Add((i).ToString("X")) 
            // printfn "Leafset for node %i is: %A " id  leafSet
            // Leafset complete

         | Join(key, entryNode) -> 
            // printfn "Key: %s joined, index: %i" key entryNode
            let mutable i = 0
            let mutable j = entryNode
            try 
               while  i < key.Length &&  key.[i] = nodeId.[i] do
                  i <- i + 1
               prefixLength <- i
               // printfn "PL: %i" prefixLength
            with 
            | :? IndexOutOfRangeException -> printfn "exception here 1 %s %s" key nodeId

            updateTable routeTable prefixLength key &j nodeId  //update table

            insertElement routeTable prefixLength key j  
            // with 
            // | :? IndexOutOfRangeException ->()

         | UpdateRouteTable (row: String[]) ->
            routeTable.[presentRow, *] <- row
            presentRow <- presentRow + 1

         | Route(key, src, hop) ->
             if nodeId = key then checkKey src hop

             elif leafSet.Contains(key) then checkKeyInLeafSet key src hop

             else routeMsg key nodeId &prefixLength routeTable src hop

         | _ -> return! loop()    
      return! loop()
   }
   loop()

let printAverageHop (averageHop: list<Double> array) (totalNumberOfHops:byref<Double>) = 
   for hop in averageHop do
      totalNumberOfHops <- totalNumberOfHops + hop.[0]
   let (total:Double) = totalNumberOfHops / double(actorHops.Count) 
   printfn "Average Hop Size %f" total

// ===== main =======
let numNodes = fsi.CommandLineArgs.[1] |> int
let numRequests  = fsi.CommandLineArgs.[2] |> int
let myActorSystem = System.create "MyActorSystem" (Configuration.load ())
let mutable nodeId = ""
let mutable hexVal = "" 
let idLength = Math.Log(numNodes |> float, (double(2.0**b))) |> ceil |> int

printfn "-------------Starting Pastry--------------"

let replicate input multiples = String.replicate input multiples // intermediate function to calculate nodeID  
nodeId <- replicate (idLength - (hexVal.Length)) "0" + hexVal
let mutable actorRef = spawn myActorSystem nodeId nodeActor
actorRef.Tell(Create(nodeId, idLength))
actorsMap <- actorsMap.Add(nodeId, actorRef)

let mutable listOfNodes : string list = []
listOfNodes <- nodeId :: listOfNodes
// printfn "List of nodes : %A" listOfNodes

let createNodes (i:int) = 
   hexVal <- i.ToString("X")  // convert to hex
   nodeId <-  replicate (idLength - (hexVal.Length)) "0" + hexVal
   // printfn "Hex:%s , HexLen: %i, nodeID: %s " hexVal hexVal.Length nodeId
   actorRef <- spawn myActorSystem nodeId nodeActor
   actorRef.Tell (Create(nodeId, idLength))
   actorsMap <- actorsMap.Add(nodeId, actorRef)
   listOfNodes <- nodeId :: listOfNodes
   // printfn "List of nodes : %A" listOfNodes

   let firstNode = replicate idLength "0"  //string: get the actor ref of the first node; lookup the actor ref in the actors map
   let startNode = actorsMap.Item firstNode
   startNode.Tell (Join(nodeId, 0))  // sending the joining nodeId and the index
   Thread.Sleep 5
   
for i in [1..numNodes - 1] do
   createNodes i

printfn "Topology Built!"
printfn "Processing requests...Please wait..."

routeRequests numRequests listOfNodes 
Thread.Sleep 100

let mutable (totalNumberOfHops:Double) = 0.0
printfn "Calculating Average hop size"

let averageHop = actorHops 
                  |> Map.toSeq 
                  |> Seq.map snd 
                  |> Seq.toArray

printAverageHop averageHop &totalNumberOfHops 
printfn "\n"

 







    




    








