{-# LANGUAGE  TemplateHaskell
, DeriveDataTypeable
, DeriveGeneric
, RankNTypes
 #-}


module Raft.RaftProtocol where

import Control.Distributed.Process.Node
import qualified Control.Distributed.Process.Extras.Timer as T
import qualified Control.Distributed.Process.Extras.Time as T
import Data.Time.Clock as T


import qualified System.Random as R

import Raft.Types

start ::  Process()
start = do
    Peers peers <- expect :: Process Peers
    followerService ((Term 0), False, []) (0,0) context peers


followerService :: RaftPersistentState -> RaftVolatileState -> Context a b -> [ProcessId] -> Process()
followerService (currentTerm, voted, logEntries) (commitIndex, lastApplied) ctx peers = do
    timeout <- getTimeout
    follower (currentTerm, False,[])  (commitIndex, lastApplied) ctx peers timeout


follower :: RaftPersistentState -> RaftVolatileState -> Context a b ->[ProcessId] -> Int -> Process()
follower (currentTerm, voted, logEntries) (commitIndex, lastApplied) ctx peers timeout = do
    mMsg <- expectMsgTimeout ctx timeout
    case mMsg of
        Nothing ->
            {- If election timeout elapses without receiving AppendEntries
               RPC from current leader or granting vote to candidate: convert
               to candidate -}
            candidateService (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers

        Just (RaftCommand _ _) -> do
            liftIO $ print "FOLLOWER"
            follower (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers timeout


        Just (Rpc m)  -> case m of
            RspVoteRPC rs -> do
                let remoteTerm = termSV rs
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        followerService ((remmote2Current remoteTerm), False, logEntries)  (commitIndex, lastApplied) ctx peers


                    termsEqualOrRemoteObsolete ->
                        follower (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers timeout


            {- The paper is vague about updating timeout for follower.
               Update it for the CurrentTermObsolete case-}
            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete -> do
                        sendVoteRPC ctx rv (current2Remote currentTerm) False
                        follower (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers timeout

                    -- TODO If election timeout elapses without receiving
                    -- AppendEntries RPC from current leader or granting vote
                    -- to candidate: convert to candidate. NOT EVRY TIME I GET REQVOT
                    TermsEqual -> do
                        sendVoteRPC ctx rv (current2Remote currentTerm) (not voted)
                        follower (currentTerm, True, logEntries)  (commitIndex, lastApplied) ctx peers timeout

                    CurrentTermObsolete -> do
                        {- Ohh I am lagging behind. I need to update my Term
                        and vote yes! -}
                        sendVoteRPC ctx rv remoteTerm True
                        followerService ((remmote2Current remoteTerm), True, logEntries)  (commitIndex, lastApplied) ctx peers

            AppendEntriesRPC ae ->
                follower ((remmote2Current (termAE ae)), voted, logEntries) (commitIndex, lastApplied) ctx peers timeout



sendVoteRPC :: Context a b-> RequestVote -> Term Remote -> Bool -> Process ()
sendVoteRPC ctx rv term voted = do
    let msg =  Rpc $ RspVoteRPC $ RspVote term voted
    sendRaftMsg ctx (candidateIdRV rv) msg



{-
A candidate wins an election if it receives votes from
a majority of the servers in the full cluster for the same
term.
-}
candidateService :: RaftPersistentState -> RaftVolatileState -> Context a b -> [ProcessId]  -> Process()
candidateService (term, voted, logEntries)  (commitIndex, lastApplied) ctx peers = do
    timeout <- getTimeout
    sPid <- getSelfPid
    let currentTerm = succTerm term
    {- Vote for self -}
    voteForMyself sPid (current2Remote currentTerm) -- TODO currentTerm - 1!
    {- Send RequestVote RPCs to all other servers -}
    sendReqVote2All sPid (current2Remote currentTerm) 0 0 peers
    {- Increment currentTerm
       Reset election timer -}
    candidate (currentTerm, True, logEntries) (commitIndex, lastApplied) ctx peers 0 timeout
    where
        sendReqVote2All sPid currentTerm ll1 ll2 peers = do
            let msg = Rpc $ ReqVoteRPC $ RequestVote currentTerm sPid ll1 ll2
            send2All ctx peers msg

        voteForMyself sPid currentTerm = do
            let msg = Rpc $ RspVoteRPC $ RspVote currentTerm True
            sendRaftMsg ctx sPid msg --sendMsg sPid msg

        succTerm :: Term Current -> Term Current
        succTerm (Term t) = Term (t+1)



candidate :: RaftPersistentState -> RaftVolatileState ->  Context a b-> [ProcessId] -> Int -> Int -> Process()
candidate (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers votes timeout = do
    mMsg <- expectMsgTimeout ctx timeout -- expectTimeout timeout :: Process (Maybe RaftMsg)
    case mMsg of
        Nothing -> do
            {- If election timeout elapses: start new election-}
            {- if many followers become candidates at the same time, votes could
               be split so that no candidate obtains a majority. When this happens,
               each candidate  will time out and start a new election by
               incrementing its term and initiating another round of RequestVote
               RPCs -}

            candidateService (currentTerm,voted, logEntries)  (commitIndex, lastApplied) ctx peers

        Just (RaftCommand _ _)  -> do
            liftIO $ print "CAND"
            candidate (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers votes timeout


        Just (Rpc m) -> case  m of
            RspVoteRPC sv -> do  -- TODO extrabt term to (Term, RCPMsg and pattern match)
                let remoteTerm = termSV sv
                    isVoteGranted = voteGrantedSV sv
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        convertToFreshFollower remoteTerm logEntries  (commitIndex, lastApplied)  ctx peers

                    termsEqualOrRemoteObsolete -> do
                        let newVotes = if isVoteGranted then votes +1 else votes
                            isLeader = newVotes > div (length peers) 2
                        if isLeader
                            then
                                leadrService currentTerm ctx peers
                            else
                                candidate (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers newVotes timeout


            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                {- If RPC request or response contains term T > currentTerm:
                  set currentTerm = T, convert to follower (§5.1-}
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        convertToFreshFollower remoteTerm logEntries  (commitIndex, lastApplied) ctx peers


                    termsEqualOrRemoteObsolete ->
                        candidate (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers votes timeout


            {- AppendEntriesRPC are send only by leaders-}
            AppendEntriesRPC ae -> do
                let remoteTerm = termAE ae
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete ->
                        {- If the term in the RPC is smaller than the candidate’s
                           current term, then the candidate rejects the RPC and
                           continues in candidate state -}
                        candidate (currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers votes timeout

                    termsEqualOrCurrentObstole ->
                        {- If the leader’s term (included in its RPC) is at
                           least as large as the candidate’s current term,
                           then the candidate recognizes the leader as legitimate
                           and returns to follower state -}
                        convertToFreshFollower remoteTerm logEntries (commitIndex, lastApplied)  ctx peers



send2All ctx peers msg =
    mapM_ (\pid -> sendRaftMsg ctx pid msg) $ peers


heartBeat ctx peers currentTerm leaderId prevLogIndex prevLogTerm entries leaderCommit=
    send2All ctx peers (Rpc $ AppendEntriesRPC $ AppendEntries currentTerm leaderId prevLogIndex prevLogTerm entries leaderCommit)


getTimeout = do
    timeout <- liftIO $ R.randomRIO (150, 300) :: Process Int
    return $ T.after timeout T.Millis


leadrService :: Term Current ->  Context a b -> [ProcessId] -> Process()
leadrService currentTerm ctx peers = do
    {- Upon election: send initial empty AppendEntries RPCs
    (heartbeat) to each server; repeat during idle periods to
    prevent election timeouts (§5.2)-}
    say $ "I am leader !!!00"

    --heartBeat ctx peers (current2Remote currentTerm)
    sPid <- getSelfPid
    say $ "I am leader !!! ------- "
    leader (currentTerm, []) (0, 0) (0,0) ctx peers
    return ()


leader st@(currentTerm, logEntries) (commitIndex, lastApplied) (nextIndex, matchIndex) ctx peers = do
    mMsg <- expectMsg ctx-- :: Process RaftMsg

    case mMsg of
        (RaftCommand clientPid _) -> do
            liftIO $ print "GOT MSG"
            rs <- stateMachineResp ctx
            sendRspToClient ctx clientPid (ClientData rs)
            return ()

        Rpc (RspVoteRPC rs) -> do
            {- If RPC request or response contains term T > currentTerm:
              set currentTerm = T, convert to follower (§5.1-}
            let remoteTerm = termSV rs
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex)  ctx peers


                termsEqualOrRemoteObsolete ->
                    leader st  (commitIndex, lastApplied) (nextIndex, matchIndex) ctx peers


        Rpc (ReqVoteRPC rv) -> do
            let remoteTerm = termRV rv
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex) ctx peers

                termsEqualOrRemoteObsolete ->
                    leader st  (commitIndex, lastApplied) (nextIndex, matchIndex) ctx peers


        Rpc (AppendEntriesRPC ae) -> do
            undefined


convertToFreshFollower remoteTerm logEntries (nextIndex, matchIndex) peers   =
    followerService ((remmote2Current remoteTerm), False, logEntries) (nextIndex, matchIndex)  peers




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
