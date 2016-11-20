{-# LANGUAGE  TemplateHaskell
, DeriveDataTypeable
, DeriveGeneric
 #-}


module Raft.RaftProtocol where


import Control.Distributed.Process
import Control.Distributed.Process.Node
import qualified Control.Distributed.Process.Extras.Timer as T
import qualified Control.Distributed.Process.Extras.Time as T
import Data.Time.Clock as T


import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import qualified System.Random as R

data Peers = Peers ![ProcessId]
    deriving (Eq, Show, Typeable, Generic)

instance Binary Peers


data Current
data Remote

-- Phantom Type
data Term a = Term !Int
    deriving (Eq, Show, Typeable, Generic)

--transfer2Remote :: Term Transfer -> Term Remote
--transfer2Remote (Term t) = Term t

remmote2Current :: Term Remote -> Term Current
remmote2Current (Term t) = Term t

--remmote2Transfer :: Term Remote -> Term Transfer
--remmote2Transfer (Term t) = Term t

current2Remote :: Term Current -> Term Remote
current2Remote (Term t) = Term t

--current2Transfer :: Term Current -> Term Transfer
--current2Transfer (Term t) = Term t


instance Binary (Term a)

data RequestVote = RequestVote
          { termRV         :: !(Term Remote)
          , candidateIdRV  :: !ProcessId
          , lastLogIndexRV :: !Int
          , lastLogTermRV  :: !Int
          }
          deriving (Eq, Show, Typeable, Generic)


instance Binary RequestVote


data RspVote = RspVote
              { termSV        :: !(Term Remote)
              , voteGrantedSV :: !Bool
              }
              deriving (Eq, Show, Typeable, Generic)


instance Binary RspVote


data Command = Command
    deriving (Eq, Show, Typeable, Generic)

instance Binary Command

data AppendEntries = AppendEntries
    { termAE       :: !(Term Remote) -- TODO refactor term
    , leaderId     :: !ProcessId
    , prevLogIndex :: !Int
    , prevLogTerm  :: !(Term Remote)
    , entries      :: !(Maybe [(Term Remote, Command)]) --log entries to store (Nothing for heartbeat;
    , leaderCommit :: !Int

    }
    deriving (Eq, Show, Typeable, Generic)

instance Binary AppendEntries


data RpcMsg = ReqVoteRPC RequestVote
            | RspVoteRPC RspVote
            | AppendEntriesRPC AppendEntries
            deriving (Eq, Show, Typeable, Generic)


instance Binary RpcMsg

data TermComparator = RemoteTermObsolete
                    | TermsEqual
                    | CurrentTermObsolete

compareTerms :: Term Remote -> Term Current -> TermComparator
compareTerms (Term remoteTerm) (Term currentTerm)
    | currentTerm >  remoteTerm = RemoteTermObsolete
    | currentTerm == remoteTerm = TermsEqual
    | currentTerm <  remoteTerm = CurrentTermObsolete



start :: Process()
start = do
    Peers peers <- expect :: Process Peers
    followerService ((Term 0), False, []) ([],[]) peers


followerService :: (Term Current, Bool, [Command]) -> ([Int], [Int]) -> [ProcessId]-> Process()
followerService (currentTerm, voted, logEntries) (nextIndex, matchIndex) peers = do
    timeout <- getTimeout
    follower (currentTerm, False,[]) (nextIndex, matchIndex) timeout peers


follower :: (Term Current, Bool, [Command]) -> ([Int], [Int]) -> Int -> [ProcessId]-> Process()
follower (currentTerm, voted, logEntries) (nextIndex, matchIndex) timeout peers = do
    mMsg <- expectTimeout timeout :: Process (Maybe RpcMsg)
    case mMsg of
        Nothing ->
            {- If election timeout elapses without receiving AppendEntries
               RPC from current leader or granting vote to candidate: convert
               to candidate -}
            candidateService (currentTerm, voted, logEntries) (nextIndex, matchIndex) peers
        Just m  -> case m of
            RspVoteRPC rs -> do
                let remoteTerm = termSV rs
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        followerService ((remmote2Current remoteTerm), False, logEntries) (nextIndex, matchIndex) peers

                    termsEqualOrRemoteObsolete ->
                        follower (currentTerm, voted, logEntries) (nextIndex, matchIndex) timeout peers


            {- The paper is vague about updating timeout for follower.
               Update it for the CurrentTermObsolete case-}
            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete -> do
                        sendVoteRPC rv (current2Remote currentTerm) False
                        follower (currentTerm, voted, logEntries) (nextIndex, matchIndex) timeout peers

                    -- TODO If election timeout elapses without receiving
                    -- AppendEntries RPC from current leader or granting vote
                    -- to candidate: convert to candidate. NOT EVRY TIME I GET REQVOT
                    TermsEqual -> do
                        sendVoteRPC rv (current2Remote currentTerm) (not voted)
                        follower (currentTerm, True, logEntries) (nextIndex, matchIndex) timeout peers

                    CurrentTermObsolete -> do
                        {- Ohh I am lagging behind. I need to update my Term
                        and vote yes! -}
                        sendVoteRPC rv remoteTerm True
                        followerService ((remmote2Current remoteTerm), True, logEntries) (nextIndex, matchIndex) peers

            AppendEntriesRPC ae ->
                follower ((remmote2Current (termAE ae)), voted, logEntries) (nextIndex, matchIndex) timeout peers

sendVoteRPC :: RequestVote -> Term Remote -> Bool -> Process ()
sendVoteRPC rv term voted =
    send (candidateIdRV rv) (RspVoteRPC $ RspVote term voted)



{-
A candidate wins an election if it receives votes from
a majority of the servers in the full cluster for the same
term.
-}
candidateService :: (Term Current, Bool, [Command]) -> ([Int], [Int]) -> [ProcessId] -> Process()
candidateService (term, voted, logEntries) (nextIndex, matchIndex) peers = do
    timeout <- getTimeout
    sPid <- getSelfPid
    let currentTerm = succTerm term
    {- Vote for self -}
    voteForMyself sPid (current2Remote currentTerm) -- TODO currentTerm - 1!
    {- Send RequestVote RPCs to all other servers -}
    sendReqVote2All sPid (current2Remote currentTerm) 0 0 peers
    {- Increment currentTerm
       Reset election timer -}
    candidate (currentTerm, True, logEntries) (nextIndex, matchIndex) 0 timeout peers
    where
        sendReqVote2All sPid currentTerm ll1 ll2 peers = do
            let msg = ReqVoteRPC $ RequestVote currentTerm sPid ll1 ll2
            send2All peers msg

        voteForMyself sPid currentTerm = do
            let msg = RspVoteRPC $ RspVote currentTerm True
            sendMsg sPid msg

        succTerm :: Term Current -> Term Current
        succTerm (Term t) = Term (t+1)



candidate :: (Term Current, Bool, [Command]) -> ([Int], [Int]) -> Int -> Int->[ProcessId] -> Process()
candidate (currentTerm, voted, logEntries) (nextIndex, matchIndex) votes timeout peers = do
    mMsg <- expectTimeout timeout :: Process (Maybe RpcMsg)
    case mMsg of
        Nothing -> do
            {- If election timeout elapses: start new election-}
            {- if many followers become candidates at the same time, votes could
               be split so that no candidate obtains a majority. When this happens,
               each candidate  will time out and start a new election by
               incrementing its term and initiating another round of RequestVote
               RPCs -}

            candidateService (currentTerm,voted, logEntries) (nextIndex, matchIndex) peers

        Just m -> case  m of
            RspVoteRPC sv -> do  -- TODO extrabt term to (Term, RCPMsg and pattern match)
                let remoteTerm = termSV sv
                    isVoteGranted = voteGrantedSV sv
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex)  peers

                    termsEqualOrRemoteObsolete -> do
                        let newVotes = if isVoteGranted then votes +1 else votes
                            isLeader = newVotes > div (length peers) 2
                        if isLeader
                            then
                                leadrService currentTerm peers
                            else
                                candidate (currentTerm, voted, logEntries) (nextIndex, matchIndex) newVotes timeout peers


            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                {- If RPC request or response contains term T > currentTerm:
                  set currentTerm = T, convert to follower (§5.1-}
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex) peers


                    termsEqualOrRemoteObsolete ->
                        candidate (currentTerm, voted, logEntries) (nextIndex, matchIndex) votes timeout peers


            {- AppendEntriesRPC are send only by leaders-}
            AppendEntriesRPC ae -> do
                let remoteTerm = termAE ae
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete ->
                        {- If the term in the RPC is smaller than the candidate’s
                           current term, then the candidate rejects the RPC and
                           continues in candidate state -}
                        candidate (currentTerm, voted, logEntries) (nextIndex, matchIndex) votes timeout peers

                    termsEqualOrCurrentObstole ->
                        {- If the leader’s term (included in its RPC) is at
                           least as large as the candidate’s current term,
                           then the candidate recognizes the leader as legitimate
                           and returns to follower state -}
                        convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex)  peers




sendMsg :: ProcessId -> RpcMsg -> Process ()
sendMsg pid rpcMsg = send pid rpcMsg


send2All peers msg =
    mapM_ (\pid -> sendMsg pid msg) $ peers


heartBeat peers currentTerm =
    send2All peers (AppendEntriesRPC $ AppendEntries currentTerm undefined undefined undefined undefined undefined)


getTimeout = do
    timeout <- liftIO $ R.randomRIO (150, 300) :: Process Int
    return $ T.after timeout T.Millis


leadrService :: Term Current ->  [ProcessId] -> Process()
leadrService currentTerm peers = do
    {- Upon election: send initial empty AppendEntries RPCs
    (heartbeat) to each server; repeat during idle periods to
    prevent election timeouts (§5.2)-}
    say $ "I am leader !!!00"

    heartBeat peers (current2Remote currentTerm)
    sPid <- getSelfPid
    say $ "I am leader !!! ------- "
    return ()


leader st@(currentTerm, logEntries) (nextIndex, matchIndex) peers = do
    mMsg <- expect :: Process RpcMsg
    case mMsg of
        RspVoteRPC rs -> do
            {- If RPC request or response contains term T > currentTerm:
              set currentTerm = T, convert to follower (§5.1-}
            let remoteTerm = termSV rs
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex)  peers


                termsEqualOrRemoteObsolete ->
                    leader st (nextIndex, matchIndex) peers


        ReqVoteRPC rv -> do
            let remoteTerm = termRV rv
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex)  peers

                termsEqualOrRemoteObsolete ->
                    leader st (nextIndex, matchIndex) peers


        AppendEntriesRPC ae -> do
            undefined


convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex) peers   =
    followerService ((remmote2Current remoteTerm), False, logEntries) (nextIndex, matchIndex)  peers


{- If RPC request or response contains term T > currentTerm:
  set currentTerm = T, convert to follower (§5.1-}

stayOrConvert2Follower (remoteTerm, currentTerm, logEntries) me (nextIndex, matchIndex)  peers =
    case compareTerms remoteTerm currentTerm of
        CurrentTermObsolete ->
            followerService ((remmote2Current remoteTerm), False, logEntries) (nextIndex, matchIndex) peers

        termsEqualOrRemoteObsolete -> me



--expectWithTimeout :: Int
--                  -> Process (Maybe (T.NominalDiffTime, RpcMsg))
expectWithTimeout timeout = do
    timeStart <- liftIO T.getCurrentTime
    m <- expectTimeout $ T.asTimeout timeout
    case m of
        Nothing ->
            return Nothing
        Just x -> do
            timeEnd <- liftIO T.getCurrentTime
            let diff =  T.diffTimeToTimeInterval $ T.diffUTCTime timeEnd timeStart
                newTimeOut = timeout - diff
            if (signum newTimeOut == -1) then
                return  Nothing
            else
                return $ Just (newTimeOut, x)

--dT :: Int
