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


    /// Using SQLite's SQLiteDataReader directly is too often permissive 
    /// as it grants access to both the traversal and the current row.
    /// Potentially we want to wrap SQLiteDataReader and just expose 
    /// the row reading functions.
    [<Struct>]
    type RowReader = 
        | RowReader of SQLiteDataReader


        member x.GetDataTypeName(i :int) : string = 
            let (RowReader reader) = x in reader.GetDataTypeName(i)
        
        member x.GetName(i :int) : string = 
            let (RowReader reader) = x in reader.GetName(i)

        member x.GetOrdinal(name :string) : int = 
            let (RowReader reader) = x in reader.GetOrdinal(name)


        member x.GetBlob(i : int, readOnly : bool) : SQLiteBlob = 
            let (RowReader reader) = x in reader.GetBlob(i, readOnly)

        member x.GetBoolean(i : int) : bool = 
            let (RowReader reader) = x in reader.GetBoolean(i)

        member x.GetByte(i : int) : byte = 
            let (RowReader reader) = x in reader.GetByte(i)
        
        member x.GetDateTime(i : int) : System.DateTime = 
            let (RowReader reader) = x in reader.GetDateTime(i)

        member x.GetDecimal(i : int) : decimal = 
            let (RowReader reader) = x in reader.GetDecimal(i)

        member x.GetDouble(i : int) : double = 
            let (RowReader reader) = x in reader.GetDouble(i)
        
        member x.GetFloat(i : int) : single = 
            let (RowReader reader) = x in reader.GetFloat(i)

        member x.GetInt16(i : int) : int16 = 
            let (RowReader reader) = x in reader.GetInt16(i)

        member x.GetInt32(i : int) : int32 = 
            let (RowReader reader) = x in reader.GetInt32(i)

        member x.GetInt64(i : int) : int64 = 
            let (RowReader reader) = x in reader.GetInt64(i)

        member x.GetString(i : int) : string = 
            let (RowReader reader) = x in reader.GetString(i)

        member x.GetValue(i : int) : obj = 
            let (RowReader reader) = x in reader.GetValue(i)
        
        
    let internal applyRowReader (proc : RowReader -> 'a) (handle : SQLiteDataReader) : Result<'a, string> = 
        try 
            proc (RowReader handle) |> Ok
        with
        | expn -> Error expn.Message

    /// Note the query must use named holes (:name).
    /// Question marks are not recognized - use IndexedCommand instead.
    type KeyedCommand = 
        val private CmdObj : SQLiteCommand
        val private Params : (string * obj) list
        
        private new (cmd : SQLiteCommand, ps : (string * obj) list) = 
            { CmdObj = cmd; Params = ps }

        new (commandText : String) = 
            { CmdObj = new SQLiteCommand(commandText = commandText); Params = [] }

        member x.GetSQLiteCommand(connection : SQLiteConnection ) : SQLiteCommand = 
            let command = x.CmdObj
            List.iter (fun (key, boxedValue) -> 
                        command.Parameters.AddWithValue(parameterName = key, value = boxedValue) |> ignore
                        ) x.Params
            command.Connection <- connection
            command

        member x.AddWithValue(paramName : string, value : obj) : KeyedCommand = 
            new KeyedCommand (cmd = x.CmdObj, ps = (paramName, value) :: x.Params )
            
    let addNamedParam (paramName : string) (value : obj) (command : KeyedCommand) : KeyedCommand = 
        command.AddWithValue(paramName, value)

    
    type IndexedCommand = 
        val private CmdObj : SQLiteCommand
        val private RevParams : SQLiteParameter list
        
        private new (cmd : SQLiteCommand, ps : SQLiteParameter list) = 
            { CmdObj = cmd; RevParams = ps }

        new (commandText : String) = 
            { CmdObj = new SQLiteCommand(commandText = commandText); RevParams = [] }

        member x.GetSQLiteCommand(connection : SQLiteConnection ) : SQLiteCommand = 
            let command = x.CmdObj
            let addParam (param1 : SQLiteParameter) : unit =
                command.Parameters.Add(parameter = param1) |> ignore

            List.foldBack (fun x acc -> addParam x; acc) 
                            x.RevParams ()
            command.Connection <- connection
            command
        
        member x.AddParam(param1 : SQLiteParameter) : IndexedCommand = 
            new IndexedCommand (cmd = x.CmdObj, ps = param1 :: x.RevParams )


    let addParam (param1 : SQLiteParameter) (command : IndexedCommand) : IndexedCommand = 
        command.AddParam(param1)


    /// F# String mapped to DbType.String
    let stringParam (str : string) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.String)
        param1.Value <- str
        param1

    /// F# bool mapped to DbType.Boolean
    let boolParam (value : bool) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Boolean)
        param1.Value <- value
        param1