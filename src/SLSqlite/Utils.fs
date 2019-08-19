// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite


module Utils =
    
    open System
    open System.Data

    open System.Data.SQLite

    // Note - there is probably some scope for a secondary interface
    // automating the command line: e.g.
    // > sqlite3 change_request.db ".output cr_schema.sql" ".schema" ".exit"
    //
    // This functionality is not availiable through System.Data.SQLite

    // Although SQLite has no native date type, DateTime seems to be 
    // covered by the System.Data.SQLite API.
    // The functions below are relics.

 

    let toIso8601String (dt : DateTime) : string = 
        dt.ToString(format = "yyyy-MM-ddThh:mm:ss")

    let parseIso8601String (source : string) : DateTime = 
        DateTime.ParseExact(s = source, format = "yyyy-MM-ddThh:mm:ss", provider = Globalization.CultureInfo.InvariantCulture)

    let tryParseIso8601String (source : string) : DateTime option = 
        try
            parseIso8601String source |> Some
        with
        | _ -> None


    /// Note - favour Parametrized Queries whci removes the need for this
    let escapeQuotes (source: string) : string = 
        source.Replace("'", "''")
 
    let stringValue (source : string) : string = 
        match source with
        | null -> ""
        | _ -> escapeQuotes source


   
    // ************************************************************************
    // 'Static' API provided by System.Data.SQLite...


    let createDatabase (dbPath : string) : Result<unit, exn> = 
        try 
            SQLite.SQLiteConnection.CreateFile (databaseFileName = dbPath) |> Ok           
        with
        | ex -> Error ex

    let getSQLiteVersion () : string = SQLite.SQLiteConnection.SQLiteVersion
