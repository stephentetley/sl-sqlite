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
    
    /// Note 
    /// System.Data.SQLite does not support getting unsigned ints 
    /// We can get an int64 and cast to uint16 or uint32 but 
    /// The cast is obviously not safe for uint64


    /// Using SQLite's SQLiteDataReader directly is too often permissive 
    /// as it grants access to both the traversal and the current row.
    /// Potentially we want to wrap SQLiteDataReader and just expose 
    /// the row reading functions.
    [<Struct>]
    type ResultItem = 
        internal | ResultItem of SQLiteDataReader


        member x.IndexInBounds (ix : int) : bool = 
            let (ResultItem reader) = x 
            ix >= 0 && ix < reader.FieldCount

        member x.IndexOutOfBounds (ix : int) : bool = not <| x.IndexInBounds(ix)

        member x.GetDataTypeName(ix : int) : string = 
            let (ResultItem reader) = x in reader.GetDataTypeName(ix)
        
        member x.GetName(ix : int) : string = 
            let (ResultItem reader) = x in reader.GetName(ix)

        member x.GetOrdinal(name : string) : int = 
            let (ResultItem reader) = x in reader.GetOrdinal(name)

        member x.IsDBNull(ix : int) : bool = 
            let (ResultItem reader) = x in reader.IsDBNull(ix)

        member x.FieldCount 
            with get () : int = let (ResultItem reader) = x in reader.FieldCount

        member x.HasRows 
            with get () : bool = let (ResultItem reader) = x in reader.HasRows


        // Get values...
        
        member x.Item
            with get (ix : int) : obj = let (ResultItem reader) = x in reader.Item(ix)

        member x.Item
            with get (name : string) : obj = let (ResultItem reader) = x in reader.Item(name)                   

        // GetBlob

        member x.GetBlob(ix : int, readOnly : bool) : SQLiteBlob = 
            let (ResultItem reader) = x in reader.GetBlob(ix, readOnly)

        member x.GetBlob(columnName : string, readOnly : bool) : SQLiteBlob = 
            let ix = x.GetOrdinal(columnName) in x.GetBlob(ix, readOnly)
            


        member x.TryGetBlob(ix : int, readOnly : bool) : SQLiteBlob option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetBlob(ix, readOnly) |> Some

        member x.TryGetBlob(columnName : string, readOnly : bool) : SQLiteBlob option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetBlob(ix, readOnly)

        // GetBoolean

        member x.GetBoolean(ix : int) : bool = 
            let (ResultItem reader) = x in reader.GetBoolean(ix)

        member x.GetBoolean(columnName : string) : bool = 
            let ix = x.GetOrdinal(columnName) in x.GetBoolean(ix)
            

        member x.TryGetBoolean(ix : int) : bool option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetBoolean(ix) |> Some
        
        member x.TryGetBoolean(columnName : string) : bool option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetBoolean(ix)


        // Byte

        member x.GetByte(ix : int) : byte = 
            let (ResultItem reader) = x in reader.GetByte(ix)
        
        member x.GetByte(columnName : string) : byte = 
            let ix = x.GetOrdinal(columnName) in x.GetByte(ix)

        member x.TryGetByte(ix : int) : byte option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetByte(ix) |> Some

        member x.TryGetByte(columnName : string) : byte option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetByte(ix)

        // DateTime

        member x.GetDateTime(ix : int) : System.DateTime = 
            let (ResultItem reader) = x in reader.GetDateTime(ix)

        member x.GetDateTime(columnName : string) : System.DateTime = 
            let ix = x.GetOrdinal(columnName) in x.GetDateTime(ix)

        member x.TryGetDateTime(ix : int) : System.DateTime option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetDateTime(ix) |> Some
        
        member x.TryGetDateTime(columnName : string) : System.DateTime option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetDateTime(ix)

        // Decimal

        member x.GetDecimal(ix : int) : decimal = 
            let (ResultItem reader) = x in reader.GetDecimal(ix)

        member x.GetDecimal(columnName : string) : decimal = 
            let ix = x.GetOrdinal(columnName) in x.GetDecimal(ix)
        
        member x.TryGetDecimal(ix : int) : decimal option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetDecimal(ix) |> Some

        member x.TryGetDecimal(columnName : string) : decimal option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetDecimal(ix)

        // Double

        member x.GetDouble(ix : int) : double = 
            let (ResultItem reader) = x in reader.GetDouble(ix)
        
        member x.GetDouble(columnName : string) : double = 
            let ix = x.GetOrdinal(columnName) in x.GetDouble(ix)

        member x.TryGetDouble(ix : int) : double option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetDouble(ix) |> Some
        
        member x.TryGetDouble(columnName : string) : double option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetDouble(ix)

        // Float 
        
        member x.GetFloat(ix : int) : single = 
            let (ResultItem reader) = x in reader.GetFloat(ix)

        member x.GetFloat(columnName : string) : single = 
            let ix = x.GetOrdinal(columnName) in x.GetFloat(ix)

        member x.TryGetFloat(ix : int) : single option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetFloat(ix) |> Some
        
        member x.TryGetFloat(columnName : string) : single option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetFloat(ix)

        // Int16

        member x.GetInt16(ix : int) : int16 = 
            let (ResultItem reader) = x in reader.GetInt16(ix)

        member x.GetInt16(columnName : string) : int16 = 
            let ix = x.GetOrdinal(columnName) in x.GetInt16(ix)

        member x.TryGetInt16(ix : int) : int16 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetInt16(ix) |> Some
        
        member x.TryGetInt16(columnName : string) : int16 option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetInt16(ix)

        // Int32

        member x.GetInt32(ix : int) : int32 = 
            let (ResultItem reader) = x in reader.GetInt32(ix)

        member x.GetInt32(columnName : string) : int32 = 
            let ix = x.GetOrdinal(columnName) in x.GetInt32(ix)

        member x.TryGetInt32(ix : int) : int32 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetInt32(ix) |> Some

        member x.TryGetInt32(columnName : string) : int32 option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetInt32(ix)

        // Int64

        member x.GetInt64(ix : int) : int64 = 
            let (ResultItem reader) = x in reader.GetInt64(ix)

        member x.GetInt64(columnName : string) : int64 = 
            let ix = x.GetOrdinal(columnName) in x.GetInt64(ix)
        
        member x.TryGetInt64(ix : int) : int64 option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetInt64(ix) |> Some

        member x.TryGetInt64(columnName : string) : int64 option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetInt64(ix)

        // Uint16

        member x.GetUint16(ix : int) : uint16 = 
            x.GetInt64(ix) |> uint16
        
        member x.GetUint16(columnName : string) : uint16 = 
            let ix = x.GetOrdinal(columnName) in x.GetUint16(ix)

        member x.TryGetUint16(ix : int) : uint16 option = 
            Option.map uint16 <| x.TryGetInt64(ix)

        member x.TryGetUint16(columnName : string) : uint16 option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetUint16(ix)

        // Uint32

        member x.GetUint32(ix : int) : uint32 = 
            x.GetInt64(ix) |> uint32

        member x.GetUint32(columnName : string) : uint32 = 
            let ix = x.GetOrdinal(columnName) in x.GetUint32(ix)
        
        member x.TryGetUint32(ix : int) : uint32 option = 
            Option.map uint32 <| x.TryGetInt64(ix)

        member x.TryGetUint32(columnName : string) : uint32 option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetUint32(ix)

        // string

        member x.GetString(ix : int) : string = 
            let (ResultItem reader) = x in reader.GetString(ix)
        
        member x.GetString(columnName : string) : string = 
            let ix = x.GetOrdinal(columnName) in x.GetString(ix)

        member x.TryGetString(ix : int) : string option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetString(ix) |> Some

        member x.TryGetString(columnName : string) : string option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetString(ix)

        // obj

        member x.GetValue(ix : int) : obj = 
            let (ResultItem reader) = x in reader.GetValue(ix)
        
        member x.GetValue(columnName : string) : obj = 
            let ix = x.GetOrdinal(columnName) in x.GetValue(ix)

        member x.TryGetValue(ix : int) : obj option = 
            let (ResultItem reader) = x in 
                if x.IndexOutOfBounds(ix) || reader.IsDBNull(ix) then
                    None 
                else 
                    reader.GetValue(ix) |> Some

        member x.TryGetValue(columnName : string) : obj option = 
            let ix = x.GetOrdinal(columnName) in x.TryGetValue(ix)


        

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

        
        static member Succeeds (proc : ResultItem -> 'a) : Strategy<bool> = 
            Strategy <| 
                fun reader -> 
                    match reader.Read () with
                    | false -> Ok false
                    | true -> 
                        match applyRowReader proc reader with
                        | Error _ -> Ok false
                        | Ok _ -> Ok true


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


