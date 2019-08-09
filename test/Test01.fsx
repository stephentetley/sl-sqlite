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

#load "..\src\SLSqlite\Utils.fs"
#load "..\src\SLSqlite\SqliteDb.fs"
open SLSqlite.SqliteDb

let localFile (relpath : string) = 
    Path.Combine(__SOURCE_DIRECTORY__, "..", relpath)

let demo01 () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let sqlCreateTable = 
        "CREATE TABLE writers (\
         name TEXT UNIQUE PRIMARY KEY NOT NULL,\
         country TEXT);"

    let sqlInsert = 
        "INSERT INTO writers(name, country) \
         VALUES \
         ('Enrique Vila-Matas', 'Spain'),\
         ('Bae Suah', 'South Korea');"

    match createDatabase dbPath with
    | Error ex -> Error ex.Message
    | Ok _ -> 
        runSqliteDb connParams 
            <| sqliteDb { 
                    let! _ = executeNonQuery sqlCreateTable
                    let! _ = executeNonQuery sqlInsert
                    return ()
                    }
    
     
     