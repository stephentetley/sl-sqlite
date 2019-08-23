// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite.Core


// Ideally this would be in the same namespace as the monad.
// Should we push the names down another level?

[<AutoOpen>]
module Wrappers =
    
    open System
    open System.Data

    open System.Data.SQLite


    type ErrMsg = string
    
    /// Using SQLite's SQLiteDataReader directly is too often permissive 
    /// as it grants access to both the traversal and the current row.
    /// Potentially we want to wrap SQLiteDataReader and just expose 
    /// the row reading functions.
    [<Struct>]
    type ResultItem = 
        internal | ResultItem of SQLiteDataReader


        member x.IndexInBounds (i : int) : bool = 
            let (ResultItem reader) = x 
            i >= 0 && i < reader.FieldCount

        member x.IndexOutOfBounds (i : int) : bool = not <| x.IndexInBounds(i)

        member x.GetDataTypeName(i : int) : string = 
            let (ResultItem reader) = x in reader.GetDataTypeName(i)
        
        member x.GetName(i : int) : string = 
            let (ResultItem reader) = x in reader.GetName(i)

        member x.GetOrdinal(name : string) : int = 
            let (ResultItem reader) = x in reader.GetOrdinal(name)

        member x.IsDBNull(i : int) : bool = 
            let (ResultItem reader) = x in reader.IsDBNull(i)

        member x.FieldCount 
            with get () : int = let (ResultItem reader) = x in reader.FieldCount

        member x.HasRows 
            with get () : bool = let (ResultItem reader) = x in reader.HasRows


        // Get values...
        
        member x.Item
            with get (i : int) : obj = let (ResultItem reader) = x in reader.Item(i)

        member x.Item
            with get (name : string) : obj = let (ResultItem reader) = x in reader.Item(name)                   

        

        member x.GetBlob(i : int, readOnly : bool) : SQLiteBlob = 
            let (ResultItem reader) = x in reader.GetBlob(i, readOnly)

        member x.TryGetBlob(i : int, readOnly : bool) : SQLiteBlob option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetBlob(i, readOnly) |> Some


        member x.GetBoolean(i : int) : bool = 
            let (ResultItem reader) = x in reader.GetBoolean(i)

        member x.TryGetBoolean(i : int) : bool option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetBoolean(i) |> Some

        member x.GetByte(i : int) : byte = 
            let (ResultItem reader) = x in reader.GetByte(i)
        

        member x.TryGetByte(i : int) : byte option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetByte(i) |> Some

        member x.GetDateTime(i : int) : System.DateTime = 
            let (ResultItem reader) = x in reader.GetDateTime(i)

        member x.TryGetDateTime(i : int) : System.DateTime option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetDateTime(i) |> Some

        member x.GetDecimal(i : int) : decimal = 
            let (ResultItem reader) = x in reader.GetDecimal(i)
        
        member x.TryGetDecimal(i : int) : decimal option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetDecimal(i) |> Some

        member x.GetDouble(i : int) : double = 
            let (ResultItem reader) = x in reader.GetDouble(i)
        
        member x.TryGetDouble(i : int) : double option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetDouble(i) |> Some

        member x.GetFloat(i : int) : single = 
            let (ResultItem reader) = x in reader.GetFloat(i)

        member x.TryGetFloat(i : int) : single option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetFloat(i) |> Some

        member x.GetInt16(i : int) : int16 = 
            let (ResultItem reader) = x in reader.GetInt16(i)

        member x.TryGetInt16(i : int) : int16 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetInt16(i) |> Some

        member x.GetInt32(i : int) : int32 = 
            let (ResultItem reader) = x in reader.GetInt32(i)

        member x.TryGetInt32(i : int) : int32 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetInt32(i) |> Some

        member x.GetInt64(i : int) : int64 = 
            let (ResultItem reader) = x in reader.GetInt64(i)
        
        member x.TryGetInt64(i : int) : int64 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetInt64(i) |> Some

        member x.GetString(i : int) : string = 
            let (ResultItem reader) = x in reader.GetString(i)

        member x.TryGetString(i : int) : string option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetString(i) |> Some

        member x.GetValue(i : int) : obj = 
            let (ResultItem reader) = x in reader.GetValue(i)
        
        member x.TryGetValue(i : int) : obj option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(i) || reader.IsDBNull(i) then
                    None 
                else 
                    reader.GetValue(i) |> Some

        member x.ValueByName (field : string) : obj =
            let (ResultItem reader) = x 
            let ix = reader.GetOrdinal(field)
            reader.GetValue(ix)

        member x.TryValueByName (field : string) : obj option =
            let (ResultItem reader) = x 
            let ix = reader.GetOrdinal(field)
            if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                None 
            else 
                reader.GetValue(ix) |> Some
        

    let internal applyRowReader (proc : ResultItem -> 'a) (handle : SQLiteDataReader) : Result<'a, string> = 
        try 
            proc (ResultItem handle) |> Ok
        with
        | expn -> Error expn.Message


    // ************************************************************************
    // Strategy wraps the interation of a SQLiteDataReader 

    type Strategy<'a> = 
        internal | Strategy of (SQLiteDataReader -> Result<'a, ErrMsg>)

        static member Map (proc : ResultItem -> 'a) : Strategy<'a list> = 
            Strategy <| 
                fun reader -> 
                let rec work fk sk = 
                    match reader.Read () with
                    | false -> sk []
                    | true -> 
                        match applyRowReader proc reader with
                        | Error msg -> fk msg
                        | Ok a1 -> 
                            work fk (fun xs -> sk (a1 :: xs))
                work (fun x -> Error x) (fun x -> Ok x)
        
        /// Synonym for Map
        static member ReadAll (proc : ResultItem -> 'a) : Strategy<'a list> = 
            Strategy.Map proc

        static member Fold (proc : 'state -> ResultItem ->  'state) 
                           (stateZero : 'state) : Strategy<'state> = 
            Strategy <| 
                fun reader -> 
                    let rec work st fk sk = 
                        match reader.Read () with
                        | false -> sk st
                        | true -> 
                            match applyRowReader (proc st) reader with
                            | Error msg -> fk msg
                            | Ok st1 -> work st1 fk sk
                    work stateZero (fun x -> Error x) (fun x -> Ok x)

        static member Head (proc : ResultItem -> 'a) : Strategy<'a> = 
            Strategy <| 
                fun reader -> 
                    match reader.Read () with
                    | false -> Error "Head - resultset is empty"
                    | true -> 
                        match applyRowReader proc reader with
                        | Error msg -> Error "Head - failed to extract a value"
                        | Ok a -> Ok a

        



    let internal applyStrategy (proc : Strategy<'a>) (handle : SQLiteDataReader) : Result<'a, string> = 
        let (Strategy fn) = proc in fn handle
        

    // ************************************************************************
    // KeyedCommand wraps SQLiteCommand 


    /// Note the query must use named holes (:name).
    /// Question marks are not recognized - use IndexedCommand instead.
    type KeyedCommand = 
        val private CmdObj : SQLiteCommand
        val private Params : (string * SQLiteParameter) list
        
        private new (cmd : SQLiteCommand, ps : (string * SQLiteParameter) list) = 
            { CmdObj = cmd; Params = ps }

        new (commandText : String) = 
            { CmdObj = new SQLiteCommand(commandText = commandText); Params = [] }

        member x.GetSQLiteCommand(connection : SQLiteConnection ) : SQLiteCommand = 
            let command = x.CmdObj
            let addParam (key : string) (param1 : SQLiteParameter) : unit =
                param1.ParameterName <- key
                command.Parameters.Add(param1) |> ignore
            List.iter (fun (key, boxedValue) -> addParam key boxedValue) x.Params
            command.Connection <- connection
            command

        member x.AddWithValue(paramName : string, value : SQLiteParameter) : KeyedCommand = 
            new KeyedCommand (cmd = x.CmdObj, ps = (paramName, value) :: x.Params )
            
    let addNamedParam (paramName : string) (value : SQLiteParameter) (command : KeyedCommand) : KeyedCommand = 
        command.AddWithValue(paramName, value)


    // ************************************************************************
    // IndexedCommand wraps SQLiteCommand 

    /// Note the query must use anonymous holes (?).
    /// Named holes (prefixed with a colon) are not recognized - use KeyedCommand instead.
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

    /// Return a parameter for null values. 
    /// Note - this has no intrinsic type.
    let nullParam () : SQLiteParameter = 
        let param1 = new SQLiteParameter()
        param1.IsNullable <- true
        param1.Value <- null
        param1
    
    
    /// F# bool mapped to DbType.Boolean
    let boolParam (value : bool) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Boolean)
        param1.Value <- value
        param1

    /// F# byte mapped to DbType.Byte
    let byteParam (value : byte) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Byte)
        param1.Value <- value
        param1


    /// F# DateTime mapped to DbType.DateTime
    let dateTimeParam (value : DateTime) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.DateTime)
        param1.Value <- value
        param1

    /// F# decimal mapped to DbType.Decimal
    let decimalParam (value : decimal) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Decimal)
        param1.Value <- value
        param1

    /// F# double mapped to DbType.Double
    let doubleParam (value : double) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Double)
        param1.Value <- value
        param1

    /// F# Guid mapped to DbType.Guid
    let guidParam (value : Guid) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Guid)
        param1.Value <- value
        param1

    /// F# int16 mapped to DbType.Int16
    let int16Param (value : int16) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Int16)
        param1.Value <- value
        param1

    /// F# int32 mapped to DbType.Int32
    let int32Param (value : int32) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Int32)
        param1.Value <- value
        param1

    /// F# int64 mapped to DbType.Int64
    let int64Param (value : int64) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Int64)
        param1.Value <- value
        param1

    /// F# single mapped to DbType.Single
    let singleParam (value : single) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.Single)
        param1.Value <- value
        param1

    /// F# string mapped to DbType.String
    /// FSharps string type can contain `null` - this is mapped to DB null
    let stringParam (value : string) : SQLiteParameter = 
        match value with 
        | null -> nullParam ()
        | _ -> 
            let param1 = new SQLiteParameter(dbType = DbType.String)
            param1.Value <- value
            param1

    /// F# uint16 mapped to DbType.UInt16
    let uint16Param (value : uint16) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.UInt16)
        param1.Value <- value
        param1

    /// F# uint32 mapped to DbType.UInt32
    let uint32Param (value : uint32) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.UInt32)
        param1.Value <- value
        param1

    /// F# uint64 mapped to DbType.UInt64
    let uint64Param (value : uint64) : SQLiteParameter = 
        let param1 = new SQLiteParameter(dbType = DbType.UInt64)
        param1.Value <- value
        param1

    /// Represent an Option value.
    /// If the values is ``Some a` then convert to a SQLiteParameter 
    /// with the supplied mapper.
    /// If the value is None then convert to null.
    let optionNull (mapper : 'a -> SQLiteParameter) (value : Option<'a>) : SQLiteParameter = 
        match value with
        | Some v1 -> mapper v1
        | None -> nullParam ()


