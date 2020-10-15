#r "nuget: Akka.FSharp"
#time "on"
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open System.Collections
open System.Collections.Generic
open System.Diagnostics

type Command =
| Done

// type Command = 
// | Ready
// | Start of int64 
// | Done
// | Continue
// | Exit

// let (unit: int64) = 1000000 |> int64 //work unit
// let mutable  (count: int64) = 0 |> int64

// let check (k:int64) (i:int64)  =
//     let mutable (sum:int64) = 0 |> int64
//     //printf "in check fn: %i\n" i
//     for (index: int64) in i..(k+i-(1 |> int64)) do 
//         sum <- sum + (index * index) 
//     //printfn"Sum: %i\n" sum
//     if (Math.Sqrt (float sum) - Math.Floor (Math.Sqrt (float sum))) = 0. then 
//         printfn "Answer is : %i" i else ()    

// let WorkerActor (N: int64) (k:int64) (mailbox: Actor<_>) =
//     let supervisor = select "/user/SupervisorActor"  mailbox.Context.System
    
//     let rec loop () = actor {
//         supervisor.Tell(Ready)
//         let! message = mailbox.Receive ()
//         match message with
//         | Start i ->
//             //printf "worker recieved msg: %i\n" i
//             for j in i..(i+unit- (1 |> int64)) do 
//                 if (j<= N) then 
//                     check k j 
//                     supervisor.Tell(Done)
//                 else supervisor.Tell(Done)
//             //printfn "loop done in worker"
//             return! loop()   
//         | _ -> ()
//     }
//     loop ()

// let SupervisorActor (N:int64) (k:int64) (mailbox: Actor<_>)  =
//     printfn "Initializing supervisor actor"
//     let stopWatch = System.Diagnostics.Stopwatch.StartNew()
//     // pre-start
//     let Worker = spawnOpt mailbox.Context "worker" (WorkerActor N k) [ SpawnOption.Router(RoundRobinPool(1000)) ]
    
//     let rec wait() = actor {
//         let! message = mailbox.Receive ()
//         match message with 
//         | Done -> count <- (count+ (1|> int64))
//         | _ -> ()
//         if(count<N) then
//             return! wait()
//         else
//             stopWatch.Stop()
//             printfn "Timer is %f" stopWatch.Elapsed.TotalMilliseconds                
//             // Console.ReadKey() |> ignore
//             mailbox.Context.System.Terminate() |> (Async.AwaitTask) |> ignore 
//     }

//     let rec loop (i) = actor {
//         //printfn "Inside supervisor %i" i
//         let! message = mailbox.Receive ()
//         match message with 
//         | Ready -> 
//             mailbox.Context.Sender.Tell (Start i)
//             if ((i + unit) <= N) then
//                 return! loop(i + unit) 
//             else return! wait()  // Added this line
//         | Done -> 
//             count <- (count+(1 |> int64))
//             //return! loop(i)
//         | _ -> return! wait()
//         if (count <= N && (i + unit) <= N) then
//             return! loop(i+unit)
//         else return! wait()
//     }
//     loop (1 |> int64) 

//      let stopWatch = System.Diagnostics.Stopwatch.StartNew()


//===== Topology ============

let gossipActor (i:int) (N:int) (topology:string)(mailbox: Actor<_>) = 
   let random = System.Random()
   let mutable neighborList = []
   let supervisor = select "/user/supervisorActor"  mailbox.Context.System
   if (topology = "line") then
      // printfn "It's a Line topology"
      // printfn "i: %i,N: %i" i N
      
      let id = i
      // printfn "%i" id
      if (id = 1) then 
         printfn "if : %i" id
         neighborList <- [id + 1] |> List.append neighborList
      elif (id = N) then 
         printfn "elif : %i" id
         neighborList <- [id - 1] |> List.append neighborList
      elif (id <> N) then
         printfn "else: %i" id
         neighborList <- [id + 1] |> List.append neighborList
         neighborList <- [id - 1] |> List.append neighborList //works
      // printfn "neighbor List: %A" neighborList

   elif (topology = "twoD" || topology = "imp2D") then
      // printfn "It's a twoD topology"
      printfn "i: %i,N: %i" i N
      
      let id = i
      let sqRoot = ((N |> double |> sqrt) + 0.5) |> floor|> int
      let row = (id-1) / sqRoot
      let col = (id-1) % sqRoot
      if(row <> 0) then  neighborList <- (id-sqRoot)::neighborList
      if(row <> (sqRoot-1)) then neighborList <- (id+sqRoot)::neighborList
      if(col <> 0) then neighborList <- (id-1)::neighborList
      if(col <> (sqRoot-1)) then neighborList <- (id+1)::neighborList
      // neighborList <- [id + 1] |> List.append neighborList
      // neighborList <- [id + var1] |> List.append neighborList 
         
   // elif (topology = "imp2D") then
   //    printfn "It's a imp2D topology"
   //    printfn "i: %i,N: %i" i N
   //    let var2 = sqrt (float(N)) |> int // check if N is perfect square
   //    let id = i
   //    let newNumNodes = var2 * var2
   //    neighborList <- [id + 1] |> List.append neighborList
   //    neighborList <- [id + var2] |> List.append neighborList 
   //    let randomNum = random.Next(0, newNumNodes)
   //    neighborList <- [randomNum] |> List.append neighborList //random node : works

   elif (topology = "full") then
      // printfn "It's a full topology"
      printfn "i: %i, N: %i" i N
      for k in 1..N do  
         if (k <> i) then 
            neighborList <- [k] |> List.append neighborList   //works
   
   printfn "neighbor List: %A" neighborList
   // printfn "going inside loop" 
   let rec loop(count:int) = actor {
      // printfn "inside loop"
      // printfn "neighbor List: %A" neighborList
      let! message = mailbox.Receive ()
      // printfn "message recvd"

      // send to only one neighbor
      if (topology <> "imp2D") then 
         let j = random.Next(0,neighborList.Length)
         printfn "j:%i" j
         // printfn "NList len: %i" neighborList.Length
         // printfn "neighborList.[j]: %i " neighborList.[j]
         (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
      else 
         let j = random.Next(0,neighborList.Length+1)
         if j<neighborList.Length then
            (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
         else
            let randomJ = random.Next(1,N+1)
            (select (sprintf "/user/%i" neighborList.[randomJ]) mailbox.Context.System) <! message

      // for nid in neighborList do
         // (select (sprintf "/user/%i" nid) mailbox.Context.System) <! message
      printfn "count: %i" count
      if count = 10 then
         printfn "finished" 
         supervisor <! Done
         printfn "Sent message to sup"
      else   
         return! loop(count+1)
   }
   loop(1)

let SupervisorActor (N:int) (mailbox: Actor<_>) =
   printfn "Starting supervisor actor, N=%i" N
   let stopWatch = System.Diagnostics.Stopwatch()
   stopWatch.Start()
   (select "/user/1" mailbox.Context.System) <! 1
   printfn "moving inside supervisor"
   // printfn "Inside sup"
   // let! msg = mailbox.Receive()   
   // printfn "recvd message from actors"

   let rec loop (count:int) = actor {
      printfn "Inside sup: count: %i"count
      let! msg = mailbox.Receive()
      printfn "recvd message from actors"
      match msg with
      | Done -> 
         printfn "Got msg"
         // if(count = N) then 
         stopWatch.Stop()
         printfn "Timer is %f" stopWatch.Elapsed.TotalMilliseconds                
         mailbox.Context.System.Terminate() |> (Async.AwaitTask) |> ignore 
         // else return! loop (count + 1)
      }
   loop 1
   

// ===== main =======
let N    = fsi.CommandLineArgs.[1] |> int
let topology = fsi.CommandLineArgs.[2] |> string
let alg      = fsi.CommandLineArgs.[3] |> string

let myActorSystem = System.create "MyActorSystem" (Configuration.load ())
//let numNodes = sqrt (float(N)) |> int

for i in 1..N do
   spawn myActorSystem (i.ToString()) ( gossipActor i N topology ) |> ignore
printfn "Actors have been created"  

let supervisor = spawn myActorSystem "supervisorActor" (SupervisorActor N) 

// supervisor <! Done

myActorSystem.WhenTerminated.Wait ()
printfn "\n"



 









    




    








