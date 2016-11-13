{-# LANGUAGE  TemplateHaskell
, DeriveDataTypeable
, DeriveGeneric
 #-}


module Raft.RaftProtocol where


import Control.Distributed.Process
import Control.Distributed.Process.Node
import Control.Distributed.Process.Extras.Timer as T
import Control.Distributed.Process.Extras.Time as T


import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import qualified System.Random as R

data Peers = Peers ![ProcessId]
    deriving (Eq, Show, Typeable, Generic)

instance Binary Peers


data RequestVote = RequestVote
          { termRV         :: !Int
          , candidateIdRV  :: !ProcessId
          , lastLogIndexRV :: !Int
          , lastLogTermRV  :: !Int
          }
          deriving (Eq, Show, Typeable, Generic)


instance Binary RequestVote


data RspVote = RspVote
              { termSV        :: !Int
              , voteGrantedSV :: !Bool
              }
              deriving (Eq, Show, Typeable, Generic)


instance Binary RspVote



data AppendEntries = HeartBeat !Int
    deriving (Eq, Show, Typeable, Generic)

instance Binary AppendEntries


data RpcMsg = ReqVoteRPC RequestVote
            | RspVoteRPC RspVote
            | AppendEntriesRPC AppendEntries
            deriving (Eq, Show, Typeable, Generic)


instance Binary RpcMsg


start :: Process()
start = do
    Peers peers <- expect :: Process Peers
    followerService 0 peers


followerService :: Int -> [ProcessId]-> Process()
followerService localTerm peers = do
    timeout <- getTimeout
    follower timeout localTerm False peers


follower timeout term voted peers = do
    --say "follower"
    mMsg <- expectTimeout timeout :: Process (Maybe RpcMsg)
    case mMsg of
        Nothing ->
    {- If election timeout elapses without receiving AppendEntries
    RPC from current leader or granting vote to candidate:
    convert to candidate -}
            candidateService term peers
        Just m  -> case m of
            RspVoteRPC _ ->
                follower timeout term voted peers

            ReqVoteRPC rv -> do
                sendVoteRPC rv term (not voted)
                follower timeout term True peers

            AppendEntriesRPC (HeartBeat term) ->
                follower timeout term voted peers


sendVoteRPC rv term voted =
    send (candidateIdRV rv) (RspVoteRPC $ RspVote term (not voted))



{-
A candidate wins an election if it receives votes from
a majority of the servers in the full cluster for the same
term.
-}
candidateService :: Int -> [ProcessId] -> Process()
candidateService localTerm peers = do
    timeout <- getTimeout
    sPid <- getSelfPid
    {-Vote for self-}
    voteForMyself sPid localTerm
    {-Send RequestVote RPCs to all other servers-}
    sendReqVote2All (localTerm + 1) sPid 0 0 peers
    {-Increment currentTerm
       Reset election timer-}
    candidate timeout peers (localTerm + 1) 0



candidate timeout peers localTerm votes = do
    mMsg <- expectTimeout timeout :: Process (Maybe RpcMsg)
    --say $ "candidate   " ++ (show votes)
    case mMsg of
        Nothing -> do
    --        say "Ellapsed"
            {- If election timeout elapses: start new election-}
            {- if many followers become candidates at the same time, votes could
            be split so that no candidate obtains a majority. When this happens,
            each candidate  will time out and start a new election by
            incrementing its term and initiating another round of RequestVote RPCs -}
            candidateService localTerm peers
        Just m -> case  m of
            ReqVoteRPC rv -> do
                if (termRV rv > localTerm)
                    then
                        {- If RPC request or response contains term T > currentTerm:
                        set currentTerm = T, convert to follower (§5.1-}
                        followerService (termRV rv) peers
                    else
                        candidate timeout peers localTerm votes

            RspVoteRPC sv -> do
                let ns = transit2NewState localTerm votes peers sv
                case ns of
                    (True, _) ->
                        {- If votes received from majority of servers:
                        become leader-}
                        leadrService localTerm peers
                    (False, newVotes) -> do
                        candidate timeout peers localTerm newVotes

            AppendEntriesRPC (HeartBeat term) -> do
                if (term >= localTerm)
                    then do
                        {- If the leader’s term (included in its RPC) is at
                        least as large as the candidate’s current term,
                        then the candidate recognizes the leader as legitimate
                        and returns to follower state -}
                        followerService term peers
                    else do
                        {- If the term in the RPC is smaller than the candidate’s
                        current term, then the candidate rejects the RPC and
                        continues in candidate state -}
                        candidate timeout peers localTerm votes


sendMsg :: ProcessId -> RpcMsg -> Process ()
sendMsg pid rpcMsg = send pid rpcMsg


voteForMyself sPid localTerm =
    sendMsg sPid (RspVoteRPC $ RspVote localTerm True)


send2All peers msg =
    mapM_ (\pid -> sendMsg pid msg) $ peers


sendReqVote2All localTerm sPid ll1 ll2 peers = do
    let msg = ReqVoteRPC $ RequestVote localTerm sPid ll1 ll2
    send2All peers msg


heartBeat peers localTerm =
    send2All peers (AppendEntriesRPC $ HeartBeat localTerm)


transit2NewState :: Int -> Int -> [ProcessId] -> RspVote  -> (Bool, Int)
transit2NewState localTerm votes peers (RspVote term vgranted)
    | grantedAndTermEq && isLeader =
        (True, votes + 1)

    | grantedAndTermEq =
        (False, votes + 1)

    | otherwise =
        (False, votes)

    where
        grantedAndTermEq = vgranted && localTerm >= term
        isLeader = votes + 1 > div (length peers) 2

getTimeout = do
    timeout <- liftIO $ R.randomRIO (150, 300) :: Process Int
    return $ T.after timeout Millis


leadrService :: Int ->  [ProcessId] -> Process()
leadrService localTerm peers = do
    {- Upon election: send initial empty AppendEntries RPCs
    (heartbeat) to each server; repeat during idle periods to
    prevent election timeouts (§5.2)-}
    heartBeat peers localTerm
    sPid <- getSelfPid
    say $ "I am leader !!!"
    return ()
