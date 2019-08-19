// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite


module Utils =
    
    open System
    open System.Data

    open System.Data.SQLite

    // SQLite has no native date type, represent data times as strings
    // in ISO 8601 format

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


    // Note - there is plenty of scope for a secondary interface
    // automating the command line: e.g.
    // > sqlite3 change_request.db ".output cr_schema.sql" ".schema" ".exit"
    //
    // This functionality is not availiable through System.Data.SQLite


    let createDatabase (dbPath : string) : Result<unit, exn> = 
        try 
            SQLite.SQLiteConnection.CreateFile (databaseFileName = dbPath) |> Ok
        with
        | ex -> Error ex