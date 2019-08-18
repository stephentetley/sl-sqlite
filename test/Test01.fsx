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

let demo02 () =
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    runSqliteDb connParams (scDeleteFrom "authors")

let demo03 () =
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
        
    let sqlInsert = 
        new SQLiteCommand  "INSERT INTO authors (name, country) \
         VALUES \
         ('Enrique Vila-Matas', 'Spain'),\
         ('Bae Suah', 'South Korea');"
    runSqliteDb connParams 
        <| sqliteDb { 
                let! _ = executeNonQuery sqlInsert
                return ()
            }

let demo04 () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let query1 = new SQLiteCommand "SELECT * FROM authors;"    
    let readRow1 (reader : RowReader) : string * string = 
        let name = reader.GetString(0)
        let country = reader.GetString(1) 
        (name, country)

    runSqliteDb connParams 
        <| sqliteDb { 
                return! executeReader query1 (readerReadAll readRow1)
            }
    
let demo02a () = 
    let dbPath = localFile @"output\authors.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    let conn = new SQLiteConnection(connParams.ConnectionString)
    conn.Open()
    let sql = @"INSERT INTO authors(name, country) VALUES (:name, :country)"
    let command = new SQLiteCommand(commandText = sql, connection = conn)
    command.Parameters.AddWithValue(parameterName = "name", value = box "Jean Echenoz") |> printfn "Param: %O"
    command.Parameters.AddWithValue(parameterName = "country", value = box "France") |> printfn "Param: %O"
    let ans = command.ExecuteNonQuery ()
    conn.Close () 
    ans

    
     