#r "nuget: Akka.FSharp"
#time "on"
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.FSharp
open Akka.Routing

type Command = 
| Ready
| Start of int64 
| Done
| Continue
| Exit

let (unit: int64) = 100000 |> int64 //work unit
let mutable  (count: int64) = 0 |> int64

let check (k:int64) (i:int64)  =
    let mutable (sum:int64) = 0 |> int64
    //printf "in check fn: %i\n" i
    for (index: int64) in i..(k+i-(1 |> int64)) do 
        sum <- sum + (index * index) 
    //printfn"Sum: %i\n" sum
    if (Math.Sqrt (float sum) - Math.Floor (Math.Sqrt (float sum))) = 0. then 
        printfn "Answer is : %i" i else ()    

let WorkerActor (N: int64) (k:int64) (mailbox: Actor<_>) =
    let supervisor = select "/user/SupervisorActor"  mailbox.Context.System
    
    let rec loop () = actor {
        supervisor.Tell(Ready)
        let! message = mailbox.Receive ()
        match message with
        | Start i ->
            //printf "worker recieved msg: %i\n" i
            for j in i..(i+unit- (1 |> int64)) do 
                if (j<= N) then 
                    check k j 
                    supervisor.Tell(Done)
                else supervisor.Tell(Done)
            //printfn "loop done in worker"
            return! loop()   
        | _ -> ()
    }
    loop ()

let SupervisorActor (N:int64) (k:int64) (mailbox: Actor<_>)  =
    printfn "Initializing supervisor actor"
    let stopWatch = System.Diagnostics.Stopwatch.StartNew()
    // pre-start
    let Worker = spawnOpt mailbox.Context "worker" (WorkerActor N k) [ SpawnOption.Router(RoundRobinPool(1000)) ]
    
    let rec wait() = actor {
        let! message = mailbox.Receive ()
        match message with 
        | Done -> count <- (count+ (1|> int64))
        | _ -> ()
        if(count<N) then
            return! wait()
        else
            stopWatch.Stop()
            printfn "Timer is %f" stopWatch.Elapsed.TotalMilliseconds                
            // Console.ReadKey() |> ignore
            mailbox.Context.System.Terminate() |> (Async.AwaitTask) |> ignore 
    }

    let rec loop (i) = actor {
        //printfn "Inside supervisor %i" i
        let! message = mailbox.Receive ()
        match message with 
        | Ready -> 
            mailbox.Context.Sender.Tell (Start i)
            if ((i + unit) <= N) then
                return! loop(i + unit) 
            else return! wait()  // Added this line
        | Done -> 
            count <- (count+(1 |> int64))
            //return! loop(i)
        | _ -> return! wait()
        if (count <= N && (i + unit) <= N) then
            return! loop(i+unit)
        else return! wait()
    }
    loop (1 |> int64) 

//let args : string array = fsi.CommandLineArgs |> Array.tail
let N: int64 = fsi.CommandLineArgs.[1] |> int64
let k: int64 = fsi.CommandLineArgs.[2] |> int64
let myActorSystem = System.create "MyActorSystem" (Configuration.load ())
let SupervisorActor1 = spawn myActorSystem "SupervisorActor" (SupervisorActor N k)
//SupervisorActor <! Start

myActorSystem.WhenTerminated.Wait ()
printfn "\n"











    




    








