﻿// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite.Core

// Note - we favour the casing "Sqlite" rather than "SQLite" then 
// we are in a less populated namespace

[<AutoOpen>]
module SqliteMonad = 

    open System.Collections.Generic
    open System.Data
    open System.Transactions
    
    open FSharp.Core

    open System.Data.SQLite


    open SLSqlite.Core
    
    type ErrMsg = string

    type SqliteConnParams = 
        { PathToDB : string 
          SQLiteVersion : string 
        }
        member x.ConnectionString : string = 
            sprintf "Data Source=%s;Version=%s;" x.PathToDB x.SQLiteVersion

    let makeConnectionString (config : SqliteConnParams) : string = 
        sprintf "Data Source=%s;Version=%s;" config.PathToDB config.SQLiteVersion

    let sqliteConnParamsVersion3 (pathToDB:string) : SqliteConnParams = 
        { PathToDB = pathToDB; SQLiteVersion = "3" }




    // SqliteDb Monad - a Reader-Error monad
    type SqliteDb<'a> = 
        SqliteDb of (SQLite.SQLiteConnection -> Result<'a, ErrMsg>)

    let inline private apply1 (ma : SqliteDb<'a>) 
                              (conn:SQLite.SQLiteConnection) : Result<'a, ErrMsg> = 
        let (SqliteDb f) = ma in f conn

    let mreturn (x:'a) : SqliteDb<'a> = SqliteDb (fun _ -> Ok x)


    let inline private bindM (ma:SqliteDb<'a>) 
                             (f : 'a -> SqliteDb<'b>) : SqliteDb<'b> =
        SqliteDb <| fun conn -> 
            match apply1 ma conn with
            | Ok a  -> apply1 (f a) conn
            | Error msg -> Error msg

    let failM (msg:string) : SqliteDb<'a> = SqliteDb (fun r -> Error msg)

    



    
    let inline private altM  (ma : SqliteDb<unit>) 
                                 (mb : SqliteDb<'b>) : SqliteDb<'b> = 
        SqliteDb <| fun conn -> 
            match apply1 ma conn, apply1 mb conn with
            | Ok _, Ok b -> Ok b
            | Error msg, _ -> Error msg
            | _, Error msg -> Error msg


    let inline private delayM (fn : unit -> SqliteDb<'a>) : SqliteDb<'a> = 
        bindM (mreturn ()) fn 

    type SqliteDbBuilder() = 
        member self.Return x        = mreturn x
        member self.Bind (p,f)      = bindM p f
        member self.Zero ()         = failM "Zero"
        member self.Combine (p,q)   = altM p q
        member self.Delay fn        = delayM fn
        member self.ReturnFrom(ma)  = ma


    let (sqliteDb:SqliteDbBuilder) = new SqliteDbBuilder()

    // SqliteDb specific operations
    let runSqliteDb (connParams : SqliteConnParams) 
                      (action : SqliteDb<'a>): Result<'a, ErrMsg> = 
        let connString = makeConnectionString connParams
        try 
            let conn = new SQLiteConnection(connString)
            conn.Open()
            let a = match action with | SqliteDb(f) -> f conn
            conn.Close()
            a
        with
        | err -> Error (sprintf "*** Exception: %s" err.Message)


    let liftOperation (operation : unit -> 'a) : SqliteDb<'a> = 
        SqliteDb <| fun _ -> 
            try 
                let ans = operation () in Ok ans
            with
                | _ -> Error "liftOperation" 

    
    let liftOperationOption (operation : unit -> 'a option) : SqliteDb<'a> = 
        SqliteDb <| fun _ -> 
            try 
                match operation () with
                | Some ans -> Ok ans
                | None -> Error "liftOperationOption"
            with
                | _ -> Error "liftOperationOption" 

    let liftOperationResult (operation : unit -> Result<'a, ErrMsg>) : SqliteDb<'a> = 
        SqliteDb <| fun _ -> 
            try 
                operation ()
            with
                | _ -> Error "liftOperationResult" 

    let liftConn (proc:SQLite.SQLiteConnection -> 'a) : SqliteDb<'a> = 
        SqliteDb <| fun conn -> 
            try 
                let ans = proc conn in Ok ans
            with
            | err -> Error err.Message

    let executeNonQuery (cmd : SQLiteCommand) : SqliteDb<int> = 
        liftConn <| fun conn -> 
            cmd.Connection <- conn
            cmd.ExecuteNonQuery ()

    let executeNonQueryKeyed (cmd : KeyedCommand) : SqliteDb<int> = 
        liftConn <| fun conn -> 
            let command = cmd.GetSQLiteCommand(conn)
            command.ExecuteNonQuery ()

    let executeNonQueryIndexed (cmd : IndexedCommand) : SqliteDb<int> = 
        liftConn <| fun conn -> 
            let command = cmd.GetSQLiteCommand(conn)
            command.ExecuteNonQuery ()

    let executeReader (cmd : SQLiteCommand) 
                      (proc : SQLite.SQLiteDataReader -> Result<'a, ErrMsg>) : SqliteDb<'a> =
        SqliteDb <| fun conn -> 
            cmd.Connection <- conn
            let reader : SQLiteDataReader = cmd.ExecuteReader()
            let ans = proc reader
            reader.Close()
            ans

    let executeReaderKeyed (cmd : KeyedCommand) 
                           (proc : SQLite.SQLiteDataReader -> Result<'a, ErrMsg>) : SqliteDb<'a> =
        SqliteDb <| fun conn -> 
            let command = cmd.GetSQLiteCommand(conn)
            let reader : SQLiteDataReader = command.ExecuteReader()
            let ans = proc reader
            reader.Close()
            ans

    let executeReaderIndexed (cmd : IndexedCommand) 
                             (proc : SQLite.SQLiteDataReader -> Result<'a, ErrMsg>) : SqliteDb<'a> =
        SqliteDb <| fun conn -> 
            let command = cmd.GetSQLiteCommand(conn)
            let reader : SQLiteDataReader = command.ExecuteReader()
            let ans = proc reader
            reader.Close()
            ans

    let withTransaction (ma : SqliteDb<'a>) : SqliteDb<'a> = 
        SqliteDb <| fun conn -> 
            let trans = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted)
            try 
                match apply1 ma conn with
                | Ok a -> trans.Commit () ; Ok a
                | Error msg -> trans.Rollback () ; Error msg
            with 
            | ex -> trans.Rollback() ; Error ( ex.ToString() )



    // ************************************************************************
    // Usual monadic operations


    // Common operations
    let fmapM (update : 'a -> 'b) (action : SqliteDb<'a>) : SqliteDb<'b> = 
        SqliteDb <| fun conn ->
          match apply1 action conn with
          | Ok a -> Ok (update a)
          | Error msg -> Error msg
       
    /// Operator for fmap.
    let ( |>> ) (action : SqliteDb<'a>) (update : 'a -> 'b) : SqliteDb<'b> = 
        fmapM update action

    /// Flipped fmap.
    let ( <<| ) (update : 'a -> 'b) (action : SqliteDb<'a>) : SqliteDb<'b> = 
        fmapM update action


    /// Haskell Applicative's (<*>)
    let apM (mf : SqliteDb<'a ->'b>) 
            (ma : SqliteDb<'a>) : SqliteDb<'b> = 
        sqliteDb { 
            let! fn = mf
            let! a = ma
            return (fn a) 
        }

    /// Operator for apM
    let ( <**> ) (ma : SqliteDb<'a -> 'b>) 
                 (mb : SqliteDb<'a>) : SqliteDb<'b> = 
        apM ma mb


    /// Perform two actions in sequence. 
    /// Ignore the results of the second action if both succeed.
    let seqL (ma : SqliteDb<'a>) (mb : SqliteDb<'b>) : SqliteDb<'a> = 
        sqliteDb { 
            let! a = ma
            let! b = mb
            return a
        }

    /// Operator for seqL
    let (.>>) (ma : SqliteDb<'a>) 
              (mb : SqliteDb<'b>) : SqliteDb<'a> = 
        seqL ma mb

    /// Perform two actions in sequence. 
    /// Ignore the results of the first action if both succeed.
    let seqR (ma : SqliteDb<'a>) (mb : SqliteDb<'b>) : SqliteDb<'b> = 
        sqliteDb { 
            let! a = ma
            let! b = mb
            return b
        }

    /// Operator for seqR
    let (>>.) (ma : SqliteDb<'a>) 
              (mb : SqliteDb<'b>) : SqliteDb<'b> = 
        seqR ma mb

    /// Bind operator
    let ( >>= ) (ma : SqliteDb<'a>) 
                (fn : 'a -> SqliteDb<'b>) : SqliteDb<'b> = 
        bindM ma fn

    /// Flipped Bind operator
    let ( =<< ) (fn : 'a -> SqliteDb<'b>) 
                (ma : SqliteDb<'a>) : SqliteDb<'b> = 
        bindM ma fn

    let kleisliL (mf : 'a -> SqliteDb<'b>)
                 (mg : 'b -> SqliteDb<'c>)
                 (source:'a) : SqliteDb<'c> = 
        sqliteDb { 
            let! b = mf source
            let! c = mg b
            return c
        }

    /// Flipped kleisliL
    let kleisliR (mf : 'b -> SqliteDb<'c>)
                 (mg : 'a -> SqliteDb<'b>)
                 (source:'a) : SqliteDb<'c> = 
        sqliteDb { 
            let! b = mg source
            let! c = mf b
            return c
        }

    
    /// Operator for kleisliL
    let (>=>) (mf : 'a -> SqliteDb<'b>)
              (mg : 'b -> SqliteDb<'c>)
              (source:'a) : SqliteDb<'c> = 
        kleisliL mf mg source


    /// Operator for kleisliR
    let (<=<) (mf : 'b -> SqliteDb<'c>)
              (mg : 'a -> SqliteDb<'b>)
              (source:'a) : SqliteDb<'c> = 
        kleisliR mf mg source

    // ************************************************************************
    // Errors

    let throwError (msg : string) : SqliteDb<'a> = 
        SqliteDb <| fun _ -> Error msg

    let swapError (newMessage : string) (ma : SqliteDb<'a>) : SqliteDb<'a> = 
        SqliteDb <| fun conn -> 
            match apply1 ma conn with
            | Ok a -> Ok a
            | Error _ -> Error newMessage

    /// Operator for flip swapError
    let ( <??> ) (action : SqliteDb<'a>) (msg : string) : SqliteDb<'a> = 
        swapError msg action
    
    let augmentError (update : string -> string) (ma:SqliteDb<'a>) : SqliteDb<'a> = 
        SqliteDb <| fun conn ->
            match apply1 ma conn with
            | Ok a -> Ok a
            | Error msg -> Error (update msg)

    /// Try to run a computation.
    /// On failure, recover or throw again with the handler.
    let attempt (ma:SqliteDb<'a>) (handler : ErrMsg -> SqliteDb<'a>) : SqliteDb<'a> = 
        SqliteDb <| fun conn ->
            match apply1 ma conn with
            | Ok a -> Ok a
            | Error msg -> apply1 (handler msg) conn




    // ************************************************************************
    // Traversals

    /// Implemented in CPS 
    let mapM (mf: 'a -> SqliteDb<'b>) 
             (source : 'a list) : SqliteDb<'b list> = 
        SqliteDb <| fun conn -> 
            let rec work (xs : 'a list) 
                         (fk : ErrMsg -> Result<'b list, ErrMsg>) 
                         (sk : 'b list -> Result<'b list, ErrMsg>) = 
                match xs with
                | [] -> sk []
                | y :: ys -> 
                    match apply1 (mf y) conn with
                    | Error msg -> fk msg
                    | Ok a1 -> 
                        work ys fk (fun acc ->
                        sk (a1::acc))
            work source (fun msg -> Error msg) (fun ans -> Ok ans)

    let forM (xs : 'a list) (fn : 'a -> SqliteDb<'b>) : SqliteDb<'b list> = mapM fn xs


    /// Implemented in CPS 
    let mapMz (mf: 'a -> SqliteDb<'b>) 
              (source : 'a list) : SqliteDb<unit> = 
        SqliteDb <| fun conn -> 
            let rec work (xs : 'a list) 
                         (fk : ErrMsg -> Result<unit, ErrMsg>) 
                         (sk : unit -> Result<unit, ErrMsg>) = 
                match xs with
                | [] -> sk ()
                | y :: ys -> 
                    match apply1 (mf y) conn with
                    | Error msg -> fk msg
                    | Ok _ -> 
                        work ys fk (fun acc ->
                        sk acc)
            work source (fun msg -> Error msg) (fun ans -> Ok ans)


    let forMz (xs : 'a list) (fn : 'a -> SqliteDb<'b>) : SqliteDb<unit> = mapMz fn xs

    let foldM (action : 'state -> 'a -> SqliteDb<'state>) 
                (state : 'state)
                (source : 'a list) : SqliteDb<'state> = 
        SqliteDb <| fun conn -> 
            let rec work (st : 'state) 
                            (xs : 'a list) 
                            (fk : ErrMsg -> Result<'state, ErrMsg>) 
                            (sk : 'state -> Result<'state, ErrMsg>) = 
                match xs with
                | [] -> sk st
                | x1 :: rest -> 
                    match apply1 (action st x1) conn with
                    | Error msg -> fk msg
                    | Ok st1 -> 
                        work st1 rest fk (fun acc ->
                        sk acc)
            work state source (fun msg -> Error msg) (fun ans -> Ok ans)



    let smapM (action : 'a -> SqliteDb<'b>) (source : seq<'a>) : SqliteDb<seq<'b>> = 
        SqliteDb <| fun conn ->
            let sourceEnumerator = source.GetEnumerator()
            let rec work (fk : ErrMsg -> Result<seq<'b>, ErrMsg>) 
                            (sk : seq<'b> -> Result<seq<'b>, ErrMsg>) = 
                if not (sourceEnumerator.MoveNext()) then 
                    sk Seq.empty
                else
                    let a1 = sourceEnumerator.Current
                    match apply1 (action a1) conn with
                    | Error msg -> fk msg
                    | Ok b1 -> 
                        work fk (fun sx -> 
                        sk (seq { yield b1; yield! sx }))
            work (fun msg -> Error msg) (fun ans -> Ok ans)

    let sforM (sx : seq<'a>) (fn : 'a -> SqliteDb<'b>) : SqliteDb<seq<'b>> = 
        smapM fn sx
    
    let smapMz (action : 'a -> SqliteDb<'b>) 
                (source : seq<'a>) : SqliteDb<unit> = 
        SqliteDb <| fun conn ->
            let sourceEnumerator = source.GetEnumerator()
            let rec work (fk : ErrMsg -> Result<unit, ErrMsg>) 
                            (sk : unit -> Result<unit, ErrMsg>) = 
                if not (sourceEnumerator.MoveNext()) then 
                    sk ()
                else
                    let a1 = sourceEnumerator.Current
                    match apply1 (action a1) conn with
                    | Error msg -> fk msg
                    | Ok _ -> 
                        work fk sk
            work (fun msg -> Error msg) (fun ans -> Ok ans)

    
    let sforMz (source : seq<'a>) (action : 'a -> SqliteDb<'b>) : SqliteDb<unit> = 
        smapMz action source
        
    let sfoldM (action : 'state -> 'a -> SqliteDb<'state>) 
                    (state : 'state)
                    (source : seq<'a>) : SqliteDb<'state> = 
        SqliteDb <| fun conn ->
            let sourceEnumerator = source.GetEnumerator()
            let rec work (st : 'state) 
                            (fk : ErrMsg -> Result<'state, ErrMsg>) 
                            (sk : 'state -> Result<'state, ErrMsg>) = 
                if not (sourceEnumerator.MoveNext()) then 
                    sk st
                else
                    let x1 = sourceEnumerator.Current
                    match apply1 (action st x1) conn with
                    | Error msg -> fk msg
                    | Ok st1 -> 
                        work st1 fk sk
            work state (fun msg -> Error msg) (fun ans -> Ok ans)


    /// Implemented in CPS 
    let mapiM (mf : int -> 'a -> SqliteDb<'b>) 
                (source : 'a list) : SqliteDb<'b list> = 
        SqliteDb <| fun conn -> 
            let rec work (xs : 'a list)
                         (count : int)
                         (fk : ErrMsg -> Result<'b list, ErrMsg>) 
                         (sk : 'b list -> Result<'b list, ErrMsg>) = 
                match xs with
                | [] -> sk []
                | y :: ys -> 
                    match apply1 (mf count y) conn with
                    | Error msg -> fk msg
                    | Ok a1 -> 
                        work ys (count+1) fk (fun acc ->
                        sk (a1::acc))
            work source 0 (fun msg -> Error msg) (fun ans -> Ok ans)


    /// Implemented in CPS 
    let mapiMz (mf : int -> 'a -> SqliteDb<'b>) 
              (source : 'a list) : SqliteDb<unit> = 
        SqliteDb <| fun conn -> 
            let rec work (xs : 'a list) 
                         (count : int)
                         (fk : ErrMsg -> Result<unit, ErrMsg>) 
                         (sk : unit -> Result<unit, ErrMsg>) = 
                match xs with
                | [] -> sk ()
                | y :: ys -> 
                    match apply1 (mf count y) conn with
                    | Error msg -> fk msg
                    | Ok _ -> 
                        work ys (count+1) fk sk
            work source 0 (fun msg -> Error msg) (fun ans -> Ok ans)

    

    let foriM (xs : 'a list) (fn : int -> 'a -> SqliteDb<'b>) : SqliteDb<'b list> = 
        mapiM fn xs

    let foriMz (xs : 'a list) (fn : int -> 'a -> SqliteDb<'b>) : SqliteDb<unit> = 
        mapiMz fn xs


    // TODO - these might be seen as 'strategies' 
   
    let readerReadAll (proc : RowReader -> 'a) : SQLite.SQLiteDataReader -> Result<'a list, ErrMsg> = 
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

            
    let readerFoldAll (proc : 'state -> RowReader ->  'state) 
                      (stateZero : 'state) : SQLite.SQLiteDataReader -> Result<'state, ErrMsg> = 
        fun reader -> 
            let rec work st fk sk = 
                match reader.Read () with
                | false -> sk st
                | true -> 
                    match applyRowReader (proc st) reader with
                    | Error msg -> fk msg
                    | Ok st1 -> 
                        work st1 fk sk
            work stateZero (fun x -> Error x) (fun x -> Ok x)
  



    // ************************************************************************
    // "Prepackaged" SQL and helpers 

    

    /// Run a ``DELETE FROM`` query
    /// To do this should check that tableName is a simple identifier, 
    /// that it isn't a system table etc.
    let scDeleteFrom (tableName:string) : SqliteDb<int> = 
        let sql = sprintf "DELETE FROM %s" tableName
        let command = new SQLiteCommand(commandText = sql)
        executeNonQuery command


    
    

    