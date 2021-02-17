#r "nuget: Akka.FSharp"
#time "on"
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp

type Command =
| Done
| Pushsum of s:double * w:double

//======================== Gossip Actor ==========================

let gossipActor (i:int) (N:int) (topology:string)(mailbox: Actor<_>) = 
   let random = System.Random()
   let mutable neighborList = []
   let supervisor = select "/user/supervisorActor"  mailbox.Context.System  //supervisor reference
   // Topology Definition
   if (topology = "line") then      
      let id = i
      if (id = 1) then 
         neighborList <- [id + 1] |> List.append neighborList
      elif (id = N) then 
         neighborList <- [id - 1] |> List.append neighborList
      elif (id <> N) then
         neighborList <- [id + 1] |> List.append neighborList
         neighborList <- [id - 1] |> List.append neighborList 
      // printfn "neighbor List: %A" neighborList

   elif (topology = "2D" || topology = "imp2D") then     
      let id = i
      let sqRoot = ((N |> double |> sqrt) + 0.5) |> floor|> int
      // printfn "sqRoot: %i" sqRoot
      let rowNum = (id - 1) / sqRoot
      let colNum = (id - 1) % sqRoot
      if(rowNum <> 0) then  
         neighborList <- (id - sqRoot)::neighborList
      // printfn "neighbor List: %A" neighborList
      if(rowNum <> (sqRoot - 1)) then 
         neighborList <- (id + sqRoot)::neighborList
      if(colNum <> 0) then 
         neighborList <- (id - 1)::neighborList
      if(colNum <> (sqRoot - 1)) then 
         neighborList <- (id + 1)::neighborList 

   elif (topology = "full") then
      for k in 1..N do  
         if (k <> i) then 
            neighborList <- [k] |> List.append neighborList   

   // Message passing
   let rec loop(count:int) = actor {
      let! message = mailbox.Receive ()
      // send to only one neighbor
      if (topology <> "imp2D") then 
         let j = random.Next(0,neighborList.Length)
         (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
      else 
         let j = random.Next(0,neighborList.Length)
         if j <= neighborList.Length then
            (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
         else
            let randomJ = random.Next(1,N+1)
            (select (sprintf "/user/%i" neighborList.[randomJ]) mailbox.Context.System) <! message
      if count = 1 then
         //printfn "finished %i" i
         supervisor <! Done

      if (count <= 10000)  then 
         return! loop(count+1)
   }
   loop(1)

// =========================== Push-Sum Actor ===============================
let pushSumActor (i:int) (N:int) (topology:string)(mailbox: Actor<_>) =
   let random = System.Random()
   let mutable neighborList = []
   let supervisor = select "/user/supervisorActor"  mailbox.Context.System
   if (topology = "line") then   
      let id = i
      if (id = 1) then 
         neighborList <- [id + 1] |> List.append neighborList
      elif (id = N) then 
         neighborList <- [id - 1] |> List.append neighborList
      elif (id <> N) then
         neighborList <- [id + 1] |> List.append neighborList
         neighborList <- [id - 1] |> List.append neighborList 

   elif (topology = "2D" || topology = "imp2D") then
      let id = i
      let sqRoot = ((N |> double |> sqrt) + 0.5) |> floor|> int
      let rowNum = (id - 1) / sqRoot
      let colNum = (id - 1) % sqRoot
      if(rowNum <> 0) then  
         neighborList <- (id - sqRoot)::neighborList
      if(rowNum <> (sqRoot - 1)) then 
         neighborList <- (id + sqRoot)::neighborList
      if(colNum <> (sqRoot - 1)) then 
         neighborList <- (id + 1)::neighborList
      if(colNum <> 0) then 
         neighborList <- (id - 1)::neighborList

   elif (topology = "full") then
      for k in 1..N do  
         if (k <> i) then 
            neighborList <- [k] |> List.append neighborList  

   // Message Passing
   let sendMsgToPeer (message:Command) (count:int) =
      // send message to only one neighbor
      if (topology <> "imp2D") then 
         let j = random.Next(0,neighborList.Length)
         (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
      else 
         let j = random.Next(0,neighborList.Length)
         if j <= neighborList.Length then
            (select (sprintf "/user/%i" neighborList.[j]) mailbox.Context.System) <! message
         else
            let randomJ = random.Next(1, N+1)
            (select (sprintf "/user/%i" neighborList.[randomJ]) mailbox.Context.System) <! message
      
   let rec wait (oldSum:double) (oldWeight:double) (count:int) = actor {
      let! msg  = mailbox.Receive()
      match msg with 
      | Pushsum (s, w) ->
          let newSum = (s + oldSum)/2.0
          let newWeight = (w + oldWeight)/2.0
          sendMsgToPeer (Pushsum (newSum, newWeight)) count
          return! wait newSum newWeight (count + 1)
      | _ -> ()             
      }

   let rec pushSum (oldSum:double) (oldWeight:double) (count:int) = actor {
      let! msg  = mailbox.Receive()
      match msg with 
      | Pushsum (s, w) -> 
         let oldRatio = oldSum / oldWeight
         let sum = (s + oldSum)/2.0
         let weight = (w + oldWeight)/2.0
         sendMsgToPeer (Pushsum (sum, weight)) count

         if (abs (sum/weight - oldRatio) < (10.0 ** -10.0) ) then 
            if (count = 3) then
               supervisor <! Done
               return! wait sum weight (count+1)
            else
               return! pushSum sum weight (count+1)     
         else return! pushSum sum weight  1
                      
      | _ -> ()             
      } 
   
   pushSum (double i) 1.0 0

// ===============  Supervisor Actor  ======================

let SupervisorActor (N:int) (alg:string) (mailbox: Actor<_>) =
   printfn "Starting supervisor actor" 
   let stopWatch = System.Diagnostics.Stopwatch()
   stopWatch.Start()
   // send a message to gossip / push-sum actors
   if (alg = "gossip") then
      (select "/user/1" mailbox.Context.System) <! 1
   elif (alg = "pushsum") then
      (select "/user/1" mailbox.Context.System) <! Pushsum (1.0, 1.0)

   let rec loop (count:int) = actor {
      let! msg = mailbox.Receive()
      match msg with
      | Done -> 
         if(count = N) then 
            stopWatch.Stop()
            printfn "Timer is %f" stopWatch.Elapsed.TotalMilliseconds                
            mailbox.Context.System.Terminate() |> (Async.AwaitTask) |> ignore 
         else return! loop (count + 1)
      | _ -> ()
      }
   loop 1
   
// ===== main =======
let mutable N = fsi.CommandLineArgs.[1] |> int
let topology  = fsi.CommandLineArgs.[2] |> string
let alg       = fsi.CommandLineArgs.[3] |> string

let myActorSystem = System.create "MyActorSystem" (Configuration.load ())
// If N is not a perfect square, make it a perfect sqaure
let root = ((N |> double |> sqrt) + 0.5) |> floor|> int
if root*root <> N then 
   N <- (root * root)

// check for the algorithm; spawn the actors
if (alg = "gossip") then    
   for i in 1..N do
      spawn myActorSystem (i.ToString()) ( gossipActor i N topology ) |> ignore
elif (alg = "pushsum") then 
   for i in 1..N do
      spawn myActorSystem (i.ToString()) ( pushSumActor i N topology ) |> ignore

printfn "Actors have been created"  

let supervisor = spawn myActorSystem "supervisorActor" (SupervisorActor N alg) 

myActorSystem.WhenTerminated.Wait ()
printfn "\n"



 









    




    








