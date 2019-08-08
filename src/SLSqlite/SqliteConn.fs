// Copyright (c) Stephen Tetley 2019
// License: BSD 3 Clause

namespace SLSqlite

// Note - we favour the casing "Sqlite" rather than "SQLite" then 
// we are in a less populated namespace

module SqliteConn = 

    open System.Collections.Generic

    open System.Data
    open System.Data.SQLite
    
    type ErrMsg = string

    type SqliteConnParams = 
        { PathToDB : string 
          SQLiteVersion : string 
        }


    let makeConnectionString (config : SqliteConnParams) : string = 
        sprintf "Data Source=%s;Version=%s;" config.PathToDB config.SQLiteVersion

    let sqliteConnParamsVersion3 (pathToDB:string) : SqliteConnParams = 
        { PathToDB = pathToDB; SQLiteVersion = "3" }




    // SqliteConn Monad - a Reader-Error monad
    type SqliteConn<'a> = 
        SqliteConn of (SQLite.SQLiteConnection -> Result<'a, ErrMsg>)

    let inline private apply1 (ma : SqliteConn<'a>) 
                              (conn:SQLite.SQLiteConnection) : Result<'a, ErrMsg> = 
        let (SqliteConn f) = ma in f conn

    let mreturn (x:'a) : SqliteConn<'a> = SqliteConn (fun _ -> Ok x)


    let inline private bindM (ma:SqliteConn<'a>) 
                             (f : 'a -> SqliteConn<'b>) : SqliteConn<'b> =
        SqliteConn <| fun conn -> 
            match apply1 ma conn with
            | Ok a  -> apply1 (f a) conn
            | Error msg -> Error msg

    let failM (msg:string) : SqliteConn<'a> = SqliteConn (fun r -> Error msg)

    let withTransaction (ma:SqliteConn<'a>) : SqliteConn<'a> = 
        SqliteConn <| fun conn -> 
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
    let inline private combineM  (ma:SqliteConn<unit>) 
                                 (mb:SqliteConn<'b>) : SqliteConn<'b> = 
        withTransaction << SqliteConn <| fun conn -> 
            match apply1 ma conn, apply1 mb conn with
            | Ok _, Ok b -> Ok b
            | Error msg, _ -> Error msg
            | _, Error msg -> Error msg


    let inline private delayM (fn : unit -> SqliteConn<'a>) : SqliteConn<'a> = 
        bindM (mreturn ()) fn 

    type SQLiteConnBuilder() = 
        member self.Return x        = mreturn x
        member self.Bind (p,f)      = bindM p f
        member self.Zero ()         = failM "Zero"
        member self.Combine (p,q)   = combineM p q
        member self.Delay fn        = delayM fn
        member self.ReturnFrom(ma)  = ma


    let (sqliteConn:SQLiteConnBuilder) = new SQLiteConnBuilder()

    // SqliteConn specific operations
    let runSqliteConnection (connParams : SqliteConnParams) 
                            (action : SqliteConn<'a>): Result<'a, ErrMsg> = 
        let connString = makeConnectionString connParams
        try 
            let conn = new SQLiteConnection(connString)
            conn.Open()
            let a = match action with | SqliteConn(f) -> f conn
            conn.Close()
            a
        with
        | err -> Error (sprintf "*** Exception: %s" err.Message)


    let liftOperation (operation : unit -> 'a) : SqliteConn<'a> = 
        SqliteConn <| fun _ -> 
            try 
                let ans = operation () in Ok ans
            with
                | _ -> Error "liftOperation" 


    let liftConn (proc:SQLite.SQLiteConnection -> 'a) : SqliteConn<'a> = 
        SqliteConn <| fun conn -> 
            try 
                let ans = proc conn in Ok ans
            with
            | err -> Error err.Message

    let executeNonQuery (sqlStatement : string) : SqliteConn<int> = 
        liftConn <| fun conn -> 
            let cmd : SQLiteCommand = new SQLiteCommand(sqlStatement, conn)
            cmd.ExecuteNonQuery ()

    let executeReader (sqlStatement : string) 
                      (proc : SQLite.SQLiteDataReader -> 'a) : SqliteConn<'a> =
        liftConn <| fun conn -> 
            let cmd : SQLiteCommand = new SQLiteCommand(sqlStatement, conn)
            let reader : SQLiteDataReader = cmd.ExecuteReader()
            let ans = proc reader
            reader.Close()
            ans


    // ************************************************************************
    // Usual monadic operations


    // Common operations
    let fmapM (update : 'a -> 'b) (action : SqliteConn<'a>) : SqliteConn<'b> = 
        SqliteConn <| fun conn ->
          match apply1 action conn with
          | Ok a -> Ok (update a)
          | Error msg -> Error msg
       
    /// Operator for fmap.
    let ( |>> ) (action : SqliteConn<'a>) (update : 'a -> 'b) : SqliteConn<'b> = 
        fmapM update action

    /// Flipped fmap.
    let ( <<| ) (update : 'a -> 'b) (action : SqliteConn<'a>) : SqliteConn<'b> = 
        fmapM update action


    /// Haskell Applicative's (<*>)
    let apM (mf : SqliteConn<'a ->'b>) 
            (ma : SqliteConn<'a>) : SqliteConn<'b> = 
        sqliteConn { 
            let! fn = mf
            let! a = ma
            return (fn a) 
        }

    /// Operator for apM
    let ( <**> ) (ma : SqliteConn<'a -> 'b>) 
                 (mb : SqliteConn<'a>) : SqliteConn<'b> = 
        apM ma mb


    /// Perform two actions in sequence. 
    /// Ignore the results of the second action if both succeed.
    let seqL (ma : SqliteConn<'a>) (mb : SqliteConn<'b>) : SqliteConn<'a> = 
        sqliteConn { 
            let! a = ma
            let! b = mb
            return a
        }

    /// Operator for seqL
    let (.>>) (ma : SqliteConn<'a>) 
              (mb : SqliteConn<'b>) : SqliteConn<'a> = 
        seqL ma mb

    /// Perform two actions in sequence. 
    /// Ignore the results of the first action if both succeed.
    let seqR (ma : SqliteConn<'a>) (mb : SqliteConn<'b>) : SqliteConn<'b> = 
        sqliteConn { 
            let! a = ma
            let! b = mb
            return b
        }

    /// Operator for seqR
    let (>>.) (ma : SqliteConn<'a>) 
              (mb : SqliteConn<'b>) : SqliteConn<'b> = 
        seqR ma mb

    /// Bind operator
    let ( >>= ) (ma : SqliteConn<'a>) 
                (fn : 'a -> SqliteConn<'b>) : SqliteConn<'b> = 
        bindM ma fn

    /// Flipped Bind operator
    let ( =<< ) (fn : 'a -> SqliteConn<'b>) 
                (ma : SqliteConn<'a>) : SqliteConn<'b> = 
        bindM ma fn

    let kleisliL (mf : 'a -> SqliteConn<'b>)
                 (mg : 'b -> SqliteConn<'c>)
                 (source:'a) : SqliteConn<'c> = 
        sqliteConn { 
            let! b = mf source
            let! c = mg b
            return c
        }

    /// Flipped kleisliL
    let kleisliR (mf : 'b -> SqliteConn<'c>)
                 (mg : 'a -> SqliteConn<'b>)
                 (source:'a) : SqliteConn<'c> = 
        sqliteConn { 
            let! b = mg source
            let! c = mf b
            return c
        }

    
    /// Operator for kleisliL
    let (>=>) (mf : 'a -> SqliteConn<'b>)
              (mg : 'b -> SqliteConn<'c>)
              (source:'a) : SqliteConn<'c> = 
        kleisliL mf mg source


    /// Operator for kleisliR
    let (<=<) (mf : 'b -> SqliteConn<'c>)
              (mg : 'a -> SqliteConn<'b>)
              (source:'a) : SqliteConn<'c> = 
        kleisliR mf mg source

    // ************************************************************************
    // Errors

    let throwError (msg : string) : SqliteConn<'a> = 
        SqliteConn <| fun _ -> Error msg

    let swapError (newMessage : string) (ma : SqliteConn<'a>) : SqliteConn<'a> = 
        SqliteConn <| fun conn -> 
            match apply1 ma conn with
            | Ok a -> Ok a
            | Error _ -> Error newMessage

    /// Operator for flip swapError
    let ( <??> ) (action : SqliteConn<'a>) (msg : string) : SqliteConn<'a> = 
        swapError msg action
    
    let augmentError (update : string -> string) (ma:SqliteConn<'a>) : SqliteConn<'a> = 
        SqliteConn <| fun conn ->
            match apply1 ma conn with
            | Ok a -> Ok a
            | Error msg -> Error (update msg)




    // ************************************************************************
    // Traversals

    /// Implemented in CPS 
    let mapM (mf: 'a -> SqliteConn<'b>) 
             (source : 'a list) : SqliteConn<'b list> = 
        SqliteConn <| fun conn -> 
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

    let forM (xs : 'a list) (fn : 'a -> SqliteConn<'b>) : SqliteConn<'b list> = mapM fn xs


    /// Implemented in CPS 
    let mapMz (mf: 'a -> SqliteConn<'b>) 
              (source : 'a list) : SqliteConn<unit> = 
        SqliteConn <| fun conn -> 
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


    let forMz (xs : 'a list) (fn : 'a -> SqliteConn<'b>) : SqliteConn<unit> = mapMz fn xs

    let foldM (action : 'state -> 'a -> SqliteConn<'state>) 
                (state : 'state)
                (source : 'a list) : SqliteConn<'state> = 
        SqliteConn <| fun conn -> 
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



    let seqMapM (action : 'a -> SqliteConn<'b>) 
                (source : seq<'a>) : SqliteConn<seq<'b>> = 
        SqliteConn <| fun conn ->
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

            
    let seqFoldM (action : 'state -> 'a -> SqliteConn<'state>) 
                    (state : 'state)
                    (source : seq<'a>) : SqliteConn<'state> = 
        SqliteConn <| fun conn ->
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

    //let mapiM (fn:int -> 'a -> SqliteConn<'b>) (xs:'a list) : SqliteConn<'b list> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.mapiM (fun ix a -> apply1 (fn ix a) conn) xs

    //let mapiMz (fn:int -> 'a -> SqliteConn<'b>) (xs:'a list) : SqliteConn<unit> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.mapiMz (fun ix a -> apply1 (fn ix a) conn) xs

    //let foriM (xs:'a list) (fn:int -> 'a -> SqliteConn<'b>) : SqliteConn<'b list> = 
    //    mapiM fn xs

    //let foriMz (xs:'a list) (fn:int -> 'a -> SqliteConn<'b>) : SqliteConn<unit> = 
    //    mapiMz fn xs


    //// Note - goes through intermediate list, see AnswerMonad.traverseM
    //let traverseM (fn: 'a -> SqliteConn<'b>) (source:seq<'a>) : SqliteConn<seq<'b>> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.traverseM (fun x -> let mf = fn x in apply1 mf conn) source

    //let traverseMz (fn: 'a -> SqliteConn<'b>) (source:seq<'a>) : SqliteConn<unit> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.traverseMz (fun x -> let mf = fn x in apply1 mf conn) source

    //let traverseiM (fn:int -> 'a -> SqliteConn<'b>) (source:seq<'a>) : SqliteConn<seq<'b>> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.traverseiM (fun ix x -> let mf = fn ix x in apply1 mf conn) source

    //let traverseiMz (fn:int -> 'a -> SqliteConn<'b>) (source:seq<'a>) : SqliteConn<unit> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.traverseiMz (fun ix x -> let mf = fn ix x in apply1 mf conn) source

    //let sequenceM (source:SqliteConn<'a> list) : SqliteConn<'a list> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.sequenceM <| List.map (fun ma -> apply1 ma conn) source

    //let sequenceMz (source:SqliteConn<'a> list) : SqliteConn<unit> = 
    //    SqliteConn <| fun conn ->
    //        AnswerMonad.sequenceMz <| List.map (fun ma -> apply1 ma conn) source

    //// Summing variants

    //let sumMapM (fn:'a -> SqliteConn<int>) (xs:'a list) : SqliteConn<int> = 
    //    fmapM List.sum <| mapM fn xs

    //let sumMapiM (fn:int -> 'a -> SqliteConn<int>) (xs:'a list) : SqliteConn<int> = 
    //    fmapM List.sum <| mapiM fn xs

    //let sumForM (xs:'a list) (fn:'a -> SqliteConn<int>) : SqliteConn<int> = 
    //    fmapM List.sum <| forM xs fn

    //let sumForiM (xs:'a list) (fn:int -> 'a -> SqliteConn<int>) : SqliteConn<int> = 
    //    fmapM List.sum <| foriM xs fn

    //let sumTraverseM (fn: 'a -> SqliteConn<int>) (source:seq<'a>) : SqliteConn<int> =
    //    fmapM Seq.sum <| traverseM fn source

    //let sumTraverseiM (fn:int -> 'a -> SqliteConn<int>) (source:seq<'a>) : SqliteConn<int> =
    //    fmapM Seq.sum <| traverseiM fn source

    //let sumSequenceM (source:SqliteConn<int> list) : SqliteConn<int> = 
    //    fmapM List.sum <| sequenceM source


    ///// The read procedure (proc) is expected to read from a single row.
    ///// WARNING - the equivalent function on PGSQLConn does not work.
    ///// Both need to be investigated.
    //let execReaderSeq (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteConn<seq<'a>> =
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
    //let execReaderList (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteConn<'a list> =
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
    //let execReaderArray (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteConn<'a []> =
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
    //let execReaderSingleton (statement:string) (proc:SQLite.SQLiteDataReader -> 'a) : SqliteConn<'a> =
    //    SqliteConn <| fun conn -> 
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


    //let withTransactionList (values:'a list) (proc1:'a -> SqliteConn<'b>) : SqliteConn<'b list> = 
    //    withTransaction (forM values proc1)


    //let withTransactionListSum (values:'a list) (proc1:'a -> SqliteConn<int>) : SqliteConn<int> = 
    //    fmapM (List.sum) <| withTransactionList values proc1


    //let withTransactionSeq (values:seq<'a>) (proc1:'a -> SqliteConn<'b>) : SqliteConn<seq<'b>> = 
    //    withTransaction (traverseM proc1 values)
    
    //let withTransactionSeqSum (values:seq<'a>) (proc1:'a -> SqliteConn<int>) : SqliteConn<int> = 
    //    fmapM (Seq.sum) <| withTransactionSeq values proc1


    // ************************************************************************
    // "Prepackaged" SQL and helpers 

    // Run a ``DELETE FROM`` query
    let deleteAllRows (tableName:string) : SqliteConn<int> = 
        let query = sprintf "DELETE FROM %s;" tableName
        executeNonQuery query


    let escapeString (source: string) : string = 
        source.Replace("'", "''")


    let stringValue (source : string) : string = 
        match source with
        | null -> ""
        | _ -> escapeString source