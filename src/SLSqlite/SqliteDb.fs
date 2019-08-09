// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite

// Note - we favour the casing "Sqlite" rather than "SQLite" then 
// we are in a less populated namespace

module SqliteDb = 

    open System.Collections.Generic

    open System.Data
    open System.Data.SQLite
    open FSharp.Core

    open SLSqlite.Utils
    
    type ErrMsg = string

    type SqliteConnParams = 
        { PathToDB : string 
          SQLiteVersion : string 
        }


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

    let withTransaction (ma : SqliteDb<'a>) : SqliteDb<'a> = 
        SqliteDb <| fun conn -> 
            let trans = conn.BeginTransaction(System.Data.IsolationLevel.ReadCommitted)
            try 
                match apply1 ma conn with
                | Ok a -> trans.Commit () ; Ok a
                | Error msg -> trans.Rollback () ; Error msg
            with 
            | ex -> trans.Rollback() ; Error ( ex.ToString() )



    /// TODO - this needs thought...
    /// If we have transactions then it is likely this should 
    /// be "first success".
    /// Currently it is ">>." (ma & mb, returning b)
    let inline private combineM  (ma : SqliteDb<unit>) 
                                 (mb : SqliteDb<'b>) : SqliteDb<'b> = 
        withTransaction << SqliteDb <| fun conn -> 
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
        member self.Combine (p,q)   = combineM p q
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


    let liftConn (proc:SQLite.SQLiteConnection -> 'a) : SqliteDb<'a> = 
        SqliteDb <| fun conn -> 
            try 
                let ans = proc conn in Ok ans
            with
            | err -> Error err.Message

    let executeNonQuery (sqlStatement : string) : SqliteDb<int> = 
        liftConn <| fun conn -> 
            let cmd : SQLiteCommand = new SQLiteCommand(sqlStatement, conn)
            cmd.ExecuteNonQuery ()

    let executeReader (sqlStatement : string) 
                      (proc : SQLite.SQLiteDataReader -> 'a) : SqliteDb<'a> =
        liftConn <| fun conn -> 
            let cmd : SQLiteCommand = new SQLiteCommand(sqlStatement, conn)
            let reader : SQLiteDataReader = cmd.ExecuteReader()
            let ans = proc reader
            reader.Close()
            ans


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



    let seqMapM (action : 'a -> SqliteDb<'b>) 
                (source : seq<'a>) : SqliteDb<seq<'b>> = 
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

            
    let seqFoldM (action : 'state -> 'a -> SqliteDb<'state>) 
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

    //let mapiM (fn:int -> 'a -> SqliteDb<'b>) (xs:'a list) : SqliteDb<'b list> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.mapiM (fun ix a -> apply1 (fn ix a) conn) xs

    //let mapiMz (fn:int -> 'a -> SqliteDb<'b>) (xs:'a list) : SqliteDb<unit> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.mapiMz (fun ix a -> apply1 (fn ix a) conn) xs

    //let foriM (xs:'a list) (fn:int -> 'a -> SqliteDb<'b>) : SqliteDb<'b list> = 
    //    mapiM fn xs

    //let foriMz (xs:'a list) (fn:int -> 'a -> SqliteDb<'b>) : SqliteDb<unit> = 
    //    mapiMz fn xs


    //// Note - goes through intermediate list, see AnswerMonad.traverseM
    //let traverseM (fn: 'a -> SqliteDb<'b>) (source:seq<'a>) : SqliteDb<seq<'b>> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.traverseM (fun x -> let mf = fn x in apply1 mf conn) source

    //let traverseMz (fn: 'a -> SqliteDb<'b>) (source:seq<'a>) : SqliteDb<unit> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.traverseMz (fun x -> let mf = fn x in apply1 mf conn) source

    //let traverseiM (fn:int -> 'a -> SqliteDb<'b>) (source:seq<'a>) : SqliteDb<seq<'b>> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.traverseiM (fun ix x -> let mf = fn ix x in apply1 mf conn) source

    //let traverseiMz (fn:int -> 'a -> SqliteDb<'b>) (source:seq<'a>) : SqliteDb<unit> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.traverseiMz (fun ix x -> let mf = fn ix x in apply1 mf conn) source

    //let sequenceM (source:SqliteDb<'a> list) : SqliteDb<'a list> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.sequenceM <| List.map (fun ma -> apply1 ma conn) source

    //let sequenceMz (source:SqliteDb<'a> list) : SqliteDb<unit> = 
    //    SqliteDb <| fun conn ->
    //        AnswerMonad.sequenceMz <| List.map (fun ma -> apply1 ma conn) source

    //// Summing variants

    //let sumMapM (fn:'a -> SqliteDb<int>) (xs:'a list) : SqliteDb<int> = 
    //    fmapM List.sum <| mapM fn xs

    //let sumMapiM (fn:int -> 'a -> SqliteDb<int>) (xs:'a list) : SqliteDb<int> = 
    //    fmapM List.sum <| mapiM fn xs

    //let sumForM (xs:'a list) (fn:'a -> SqliteDb<int>) : SqliteDb<int> = 
    //    fmapM List.sum <| forM xs fn

    //let sumForiM (xs:'a list) (fn:int -> 'a -> SqliteDb<int>) : SqliteDb<int> = 
    //    fmapM List.sum <| foriM xs fn

    //let sumTraverseM (fn: 'a -> SqliteDb<int>) (source:seq<'a>) : SqliteDb<int> =
    //    fmapM Seq.sum <| traverseM fn source

    //let sumTraverseiM (fn:int -> 'a -> SqliteDb<int>) (source:seq<'a>) : SqliteDb<int> =
    //    fmapM Seq.sum <| traverseiM fn source

    //let sumSequenceM (source:SqliteDb<int> list) : SqliteDb<int> = 
    //    fmapM List.sum <| sequenceM source


    ///// The read procedure (proc) is expected to read from a single row.
    ///// WARNING - the equivalent function on PGSQLConn does not work.
    ///// Both need to be investigated.
    //let execReaderSeq (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteDb<seq<'a>> =
    //    liftConn <| fun conn -> 
    //        let cmd : SQLiteCommand = new SQLiteCommand(statement, conn)
    //        let reader = cmd.ExecuteReader()
    //        let resultset = 
    //            seq { while reader.Read() do
    //                    let ans = proc reader
    //                    yield ans }
    //        reader.Close()
    //        resultset

    //// The read procedure (proc) is expected to read from a single row.
    //let execReaderList (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteDb<'a list> =
    //    liftConn <| fun conn -> 
    //        let cmd : SQLiteCommand = new SQLiteCommand(statement, conn)
    //        let reader = cmd.ExecuteReader()
    //        let resultset = 
    //            seq { while reader.Read() do
    //                    let ans = proc reader
    //                    yield ans } |> Seq.toList
    //        reader.Close()
    //        resultset

    //// The read procedure (proc) is expected to read from a single row.
    //let execReaderArray (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteDb<'a []> =
    //    liftConn <| fun conn -> 
    //        let cmd : SQLiteCommand = new SQLiteCommand(statement, conn)
    //        let reader = cmd.ExecuteReader()
    //        let resultset = 
    //            seq { while reader.Read() do
    //                    let ans = proc reader
    //                    yield ans } |> Seq.toArray
    //        reader.Close()
    //        resultset



    //// The read procedure (proc) is expected to read from a single row.
    //// The query should return exactly one row.
    //let execReaderSingleton (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteDb<'a> =
    //    SqliteDb <| fun conn -> 
    //        try 
    //            let cmd : SQLiteCommand = new SQLiteCommand(statement, conn)
    //            let reader = cmd.ExecuteReader()
    //            if reader.Read() then
    //                let ans = proc reader
    //                let hasMore =  reader.Read()
    //                reader.Close()
    //                if not hasMore then
    //                    Ok <| ans
    //                else 
    //                    Err <| "execReaderSingleton - too many results."
    //            else
    //                reader.Close ()
    //                Err <| "execReaderSingleton - no results."
    //        with
    //        | ex -> Err(ex.ToString())


    //let withTransactionList (values:'a list) (proc1:'a -> SqliteDb<'b>) : SqliteDb<'b list> = 
    //    withTransaction (forM values proc1)


    //let withTransactionListSum (values:'a list) (proc1:'a -> SqliteDb<int>) : SqliteDb<int> = 
    //    fmapM (List.sum) <| withTransactionList values proc1


    //let withTransactionSeq (values:seq<'a>) (proc1:'a -> SqliteDb<'b>) : SqliteDb<seq<'b>> = 
    //    withTransaction (traverseM proc1 values)
    
    //let withTransactionSeqSum (values:seq<'a>) (proc1:'a -> SqliteDb<int>) : SqliteDb<int> = 
    //    fmapM (Seq.sum) <| withTransactionSeq values proc1


    // ************************************************************************
    // "Prepackaged" SQL and helpers 

    let createDatabase (dbPath : string) : Result<unit, exn> = 
        try 
            SQLite.SQLiteConnection.CreateFile (databaseFileName = dbPath) |> Ok
        with
        | ex -> Error ex

    // Run a ``DELETE FROM`` query
    let scDeleteFrom (tableName:string) : SqliteDb<int> = 
        let query = sprintf "DELETE FROM %s;" tableName
        executeNonQuery query


    


    let stringValue (source : string) : string = 
        match source with
        | null -> ""
        | _ -> escapeQuotes source