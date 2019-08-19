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
open SLSqlite.Utils
open SLSqlite.SqliteDb

let localFile (relpath : string) = 
    Path.Combine(__SOURCE_DIRECTORY__, "..", relpath)

let demo01 () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let sqlCreateTable = 
        new SQLiteCommand "CREATE TABLE authors (\
         name TEXT UNIQUE PRIMARY KEY NOT NULL,\
         country TEXT);"
    match createDatabase dbPath with
    | Error ex -> Error ex.Message
    | Ok _ -> 
        runSqliteDb connParams 
            <| sqliteDb { 
                    let! _ = executeNonQuery sqlCreateTable
                    return ()
                }


// This has been causing trouble, we were trying to bind tableName
// as a query paramter which isn't allowed by SQLite.
// scDeleteFrom now fixed.
let demo02 () =
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    runSqliteDb connParams (scDeleteFrom "authors")


/// TODO - use a param query ...
/// I'm not sure the Prepare() in System.Data.SQLite is valuable for SQLite and this needs examining...
let demo03 () =
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
        
    let authors = 
        [ ("Enrique Vila-Matas", "Spain")
        ; ("Bae Suah", "South Korea") 
        ]
    let dbInsert (name : string, country) : SqliteDb<int> = 
        let cmd = 
            new KeyedCommand  "INSERT INTO authors (name, country) VALUES (:name, :country)"
                |> addNamedParam "name" (stringParam name) 
                |> addNamedParam "country" (stringParam country)
        executeNonQueryKeyed cmd

    runSqliteDb connParams 
        <| sqliteDb { 
                let! xs = mapM dbInsert authors
                return xs
            }

let demo04 () =
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
        
    let authors = 
        [ ("Catherine Leroux", Some "Canada")
        ; ("Quim Monzo", Some "Spain") 
        ; ("Bad Row", None)
        ]
    let dbInsert (name : string, country : string option) : SqliteDb<int> = 
        let cmd = 
            new IndexedCommand  "INSERT INTO authors (name, country) VALUES (?,?)"
                |> addParam (stringParam name) 
                |> addParam (optionNull stringParam country)
        executeNonQueryIndexed cmd

    runSqliteDb connParams 
        <| sqliteDb { 
                let! xs = mapM dbInsert authors
                return xs
            }

let demo05 () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let query1 = new SQLiteCommand "SELECT * FROM authors;"    
    let readRow1 (reader : RowReader) : string * string option = 
        let name = reader.GetString(0)
        let country = reader.TryGetString(1) 
        (name, country)

    runSqliteDb connParams 
        <| sqliteDb { 
                return! executeReader query1 (readerReadAll readRow1)
            }
    
let demo04a () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let conn = new SQLiteConnection(connParams.ConnectionString)
    conn.Open()
    let sql = @"INSERT INTO authors(name, country) VALUES (?,?)"
    let command = 
        new SQLiteCommand(commandText = sql, connection = conn)
    
    let p1 = new SQLiteParameter(dbType = DbType.String)
    p1.Value <- "Jean Echenoz"
    command.Parameters.Add (parameter = p1)  |> printfn "%O"

    let p2 = new SQLiteParameter(dbType = DbType.String)
    p2.Value <- "France"
    command.Parameters.Add (parameter = p2) |> printfn "%O"
    let ans = command.ExecuteNonQuery ()
    conn.Close () 
    ans

    
     