// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

#r "netstandard"
#r "System.Xml.Linq.dll"
#r "System.Transactions.dll"
open System
open System.IO

#I @"C:\Users\stephen\.nuget\packages\System.Data.SQLite.Core\1.0.111\lib\netstandard2.0"
#r "System.Data.SQLite.dll"
open System.Data.SQLite
open System.Data

// A hack to get over Dll loading error due to the native dll `SQLite.Interop.dll`
[<Literal>] 
let SQLiteInterop = @"C:\Users\stephen\.nuget\packages\System.Data.SQLite.Core\1.0.111\runtimes\win-x64\native\netstandard2.0"
Environment.SetEnvironmentVariable("PATH", 
    Environment.GetEnvironmentVariable("PATH") + ";" + SQLiteInterop
    )

// SQLite potentially defines type that clashes with 
// FSharp's Result (OK + Error) type.
// Open FSharp.Core
open FSharp.Core
    
#load "..\src\SLSqlite\Core\Wrappers.fs"
#load "..\src\SLSqlite\Core\SqliteMonad.fs"
#load "..\src\SLSqlite\Utils.fs"
open SLSqlite.Core
open SLSqlite.Utils





let localFile (relpath : string) = 
    Path.Combine(__SOURCE_DIRECTORY__, "..", relpath)

let dbPath () = localFile @"output\ranks.sqlite"

let runRankTest (action : SqliteDb<'a>) : Result<'a, ErrMsg> = 
    let connParams = sqliteConnParamsVersion3 (dbPath ())
    runSqliteDb connParams action


let test01 () : Result<unit, ErrMsg> = 
    let action = sqliteDb.Zero() <?> "This should override..."
    runRankTest action

let mysprintf fmt = 
    Printf.ksprintf (fun str -> sprintf "HELLO: %s" str) fmt

let throwErrorf fmt = 
    SqliteDb <| fun _ -> 
        Error (Printf.ksprintf throwError fmt)

//// Won't compile...
//let test02 () = 
//    sqliteDb {
//        do! (throwErrorf "%i") int
//        return ()
//    }

let test03 () = 
    let action : SqliteDb<unit> = 
        sqliteDb.Zero() |?%>> ("The error was: %s" : ErrorAugmentFormat)
    runRankTest action

/// Happily this cannot type the format string and it throws the error
/// we would want it to...
//let test04 () = 
//    let action : SqliteDb<unit> = 
//        sqliteDb.Zero() |?%>> ("The error was: %i" : ErrorAugmentFormat)
//    runRankTest action


let test05 () : Result<unit, ErrMsg> = 
    getOptional (mreturn None) |> runRankTest 


// Should give a runtime failure showing the query
let test06 () = 
    let sql = 
        """
        SELECT score FROM rank, WHERE name = :name;
        """
    let cmd = 
        new KeyedCommand (commandText = sql)    
            |> addNamedParam "name" (stringParam "stephen")

    let readRow1 (result : ResultItem) = result.GetValue(0)

    let action = queryKeyed cmd (Strategy.Head readRow1) 

    runRankTest action





