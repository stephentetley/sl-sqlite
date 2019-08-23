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


let booksConnectionSettings () : SqliteConnParams =
    let dbPath = localFile @"output\books.sqlite"
    sqliteConnParamsVersion3 dbPath

let catalogue : (string * string) list = 
    [ ("North Station", "Bae Suah")
    ; ("The Illogic of Kassel", "Enrique Vila-Matas")
    ; ("Because She Never Asked", "Enrique Vila-Matas")
    ; ("Cherokee", "Jean Echenoz")
    ]



let runBooksTest (action : SqliteDb<'a>) : Result<'a, ErrMsg> = 
    let dbPath = localFile @"output\books.sqlite"
    let connParams = sqliteConnParamsVersion3 dbPath
    runSqliteDb connParams action


let setupDb () = 
    let dbPath = localFile @"output\books.sqlite"
    
    match createDatabase dbPath with
    | Error ex -> Error ex.Message
    | Ok _ -> 

        let dbCreate () : SqliteDb<int> = 
            let sql = 
                """
                CREATE TABLE books (
                  book TEXT UNIQUE PRIMARY KEY NOT NULL,
                  author TEXT
                );
                """
            executeNonQuery (new SQLiteCommand (commandText = sql))

        let dbInsert1 (book, author) : SqliteDb<int> = 
            let sql = 
                "INSERT INTO books (book, author) VALUES (?,?)"
            let cmd = 
                new IndexedCommand (commandText = sql)
                    |> addParam (stringParam book) 
                    |> addParam (stringParam author)
            executeNonQueryIndexed cmd

        runBooksTest 
            <| sqliteDb { 
                    let! a = dbCreate ()
                    let! b = 
                        if a = 0 then
                            mapM dbInsert1 catalogue |>> List.sum
                        else
                            mreturn 0
                    return (a,b)
                }

let query01 (author : string) : SqliteDb<string> = 
    let sql = 
        """
        SELECT book FROM books WHERE author = ?;
        """
    let cmd = 
        new IndexedCommand (commandText = sql)
            |> addParam (stringParam author)

    let readRow1 (result : ResultItem) : string = result.GetString(0)

    queryIndexed cmd (Strategy.Head readRow1)


