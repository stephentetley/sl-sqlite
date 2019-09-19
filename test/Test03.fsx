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


let wrap (value : uint16) : obj = 
    value :> obj

let unwrap (object : obj) : uint16 option = 
    match object with
    | :? uint16 as x -> Some x
    | _ -> None

let demo01 () = 
    wrap 1001us |> unwrap




let localFile (relpath : string) = 
    Path.Combine(__SOURCE_DIRECTORY__, "..", relpath)

let dbPath () = localFile @"output\ranks.sqlite"

let runRankTest (action : SqliteDb<'a>) : Result<'a, ErrMsg> = 
    let connParams = sqliteConnParamsVersion3 (dbPath ())
    runSqliteDb connParams action


let setupDb () : SqliteDb<unit> = 
    let ddl = 
        """
        CREATE TABLE rank (
          name TEXT UNIQUE PRIMARY KEY NOT NULL,
          score BIGINT
        );
        """
    let insert = 
        """
        INSERT INTO rank (name, score) VALUES ('gold', 10001);
        INSERT INTO rank (name, score) VALUES ('silver', 1001);
        INSERT INTO rank (name, score) VALUES ('bronze', 101);
        """
    sqliteDb { 
        let! _ = executeNonQuery (new SQLiteCommand (commandText = ddl))
        let! _ = executeNonQuery (new SQLiteCommand (commandText = insert))
        return ()
    }


let query01 () : SqliteDb<obj> = 
    let sql = 
        """
        SELECT score FROM rank WHERE name = 'gold';
        """
    let cmd = 
        new SQLiteCommand (commandText = sql)

    let readRow1 (result : ResultItem) = result.GetValue(0)

    queryCommand cmd (Strategy.Head readRow1)


let test01 () = 
    let db = dbPath ()
    match createDatabase db with
    | Error ex -> Error ex.Message
    | Ok _ -> 
        runRankTest <| 
            sqliteDb { 
                do! setupDb ()
                return! query01 ()
            }




