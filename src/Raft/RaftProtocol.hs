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
    let rs = RaftState (0, False, []) (0,0) context
    followerService rs peers

followerService :: RaftState a b -> [ProcessId] -> Process()
followerService rs peers = do
    timeout <- getTimeout
    follower rs peers timeout


follower ::  RaftState a b -> [ProcessId] -> Int -> Process()
follower raftState peers timeout = do
    let currentTerm = getTerm raftState
        ctx = getContext raftState
    mMsg <- expectMsgTimeout ctx timeout
    case mMsg of
        Nothing -> do
            --liftIO $ print "CAND"

            {- If election timeout elapses without receiving AppendEntries
               RPC from current leader or granting vote to candidate: convert
               to candidate -}
            candidateService raftState peers
            --(currentTerm, voted, logEntries)  (commitIndex, lastApplied) ctx peers
        --    undefined
        Just (RaftCommand _ _) -> do
            liftIO $ print "FOLLOWER"
            follower raftState peers timeout


        Just (Rpc m)  -> case m of
            RspVoteRPC rs -> do
                let remoteTerm = termSV rs
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        followerService (updateVoted False raftState) peers



                    termsEqualOrRemoteObsolete ->
                        follower raftState peers timeout


            {- The paper is vague about updating timeout for follower.
               Update it for the CurrentTermObsolete case-}
            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete -> do
                        sendVoteRPC ctx rv currentTerm False
                        follower raftState peers timeout

                    -- TODO If election timeout elapses without receiving
                    -- AppendEntries RPC from current leader or granting vote
                    -- to candidate: convert to candidate. NOT EVRY TIME I GET REQVOT
                    TermsEqual -> do
                        sendVoteRPC ctx rv currentTerm (not (getVoted raftState))--(not voted)
                        follower (updateVoted True raftState) peers timeout

                    CurrentTermObsolete -> do
                        {- Ohh I am lagging behind. I need to update my Term
                        and vote yes! -}
                        sendVoteRPC ctx rv remoteTerm True

                        let updateTerm2Remote = updateTerm remoteTerm
                            updateVote2True   = updateVoted True
                            newState = ( updateVote2True
                                       . updateTerm2Remote ) raftState

                        followerService newState peers


            AppendEntriesRPC ae -> do
                let remoteTerm = termAE ae
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete -> do
                        let rsp = Rpc $ RspAppendEntriesRPC $ RspAppendEntries currentTerm False
                        sendRaftMsg ctx (leaderId ae) rsp
                        followerService raftState peers


                    TermsEqual -> do
                        let ci = getCommitIndex raftState
                            la = getLastApplied raftState


                        followerService raftState peers


                    CurrentTermObsolete -> do
                        let newState = updateTerm remoteTerm raftState
                        followerService newState peers


                let newState = updateTerm (termAE ae) raftState
                follower raftState peers timeout

            RspAppendEntriesRPC rspAE -> undefined


sendVoteRPC :: Context a b -> RequestVote -> Term -> Bool -> Process ()
sendVoteRPC ctx rv term voted = do
    let msg =  Rpc $ RspVoteRPC $ RspVote term voted
    sendRaftMsg ctx (candidateIdRV rv) msg



{-
A candidate wins an election if it receives votes from
a majority of the servers in the full cluster for the same
term.
-}
candidateService :: RaftState a b -> [ProcessId]  -> Process()
candidateService raftState peers = do
    timeout <- getTimeout
    sPid <- getSelfPid
    let term = getTerm raftState
        currentTerm = term + 1
        ctx = getContext raftState
    {- Vote for self -}
    voteForMyself sPid ctx currentTerm -- TODO currentTerm - 1!
    {- Send RequestVote RPCs to all other servers -}
    sendReqVote2All sPid ctx currentTerm 0 0 peers
    {- Increment currentTerm
       Reset election timer -}

    let updateTermS = updateTerm currentTerm
        updateVote2True  = updateVoted True

        newState = ( updateVote2True
                   . updateTermS ) raftState

    candidate newState peers 0 timeout
    where
        sendReqVote2All sPid ctx currentTerm ll1 ll2  peers = do
            let msg = Rpc $ ReqVoteRPC $ RequestVote currentTerm sPid ll1 ll2
            send2All ctx peers msg

        voteForMyself sPid ctx currentTerm = do
            let msg = Rpc $ RspVoteRPC $ RspVote currentTerm True
            sendRaftMsg ctx sPid msg --sendMsg sPid msg

    --    succTerm :: Term Current -> Term Current
    --    succTerm (Term t) = Term (t+1)



candidate :: RaftState a b -> [ProcessId] -> Int -> Int -> Process()
candidate raftState peers votes timeout = do
    let ctx = getContext raftState
        currentTerm = getTerm raftState
    mMsg <- expectMsgTimeout ctx timeout -- expectTimeout timeout :: Process (Maybe RaftMsg)
    case mMsg of
        Nothing -> do
            {- If election timeout elapses: start new election-}
            {- if many followers become candidates at the same time, votes could
               be split so that no candidate obtains a majority. When this happens,
               each candidate  will time out and start a new election by
               incrementing its term and initiating another round of RequestVote
               RPCs -}

            candidateService raftState peers

        Just (RaftCommand _ _)  -> do
        --    liftIO $ print "CAND222"
            candidate raftState peers votes timeout


        Just (Rpc m) -> case  m of
            RspVoteRPC sv -> do  -- TODO extrabt term to (Term, RCPMsg and pattern match)
                let remoteTerm = termSV sv
                    isVoteGranted = voteGrantedSV sv

                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete -> do
                        let updateTerm2Remote = updateTerm remoteTerm
                            updateVote2False   = updateVoted False

                            newState = ( updateVote2False
                                       . updateTerm2Remote ) raftState

                        liftIO $ print $ (show remoteTerm) ++" "++(show currentTerm)

                        followerService newState peers

                    termsEqualOrRemoteObsolete -> do
                        let newVotes = if isVoteGranted then votes + 1 else votes
                            isLeader = newVotes > div (length peers) 2
                        if isLeader
                            then
                                leadrService raftState peers

                            else
                                candidate raftState peers newVotes timeout


            ReqVoteRPC rv -> do
                let remoteTerm = termRV rv
                {- If RPC request or response contains term T > currentTerm:
                  set currentTerm = T, convert to follower (§5.1-}
                case compareTerms remoteTerm currentTerm of
                    CurrentTermObsolete ->
                        convertToFreshFollower raftState remoteTerm peers


                    termsEqualOrRemoteObsolete ->
                        candidate raftState peers votes timeout


            {- AppendEntriesRPC are send only by leaders-}
            AppendEntriesRPC ae -> do
                let remoteTerm = termAE ae
                case compareTerms remoteTerm currentTerm of
                    RemoteTermObsolete ->
                        {- If the term in the RPC is smaller than the candidate’s
                           current term, then the candidate rejects the RPC and
                           continues in candidate state -}
                        candidate raftState peers votes timeout

                    termsEqualOrCurrentObstole ->
                        {- If the leader’s term (included in its RPC) is at
                           least as large as the candidate’s current term,
                           then the candidate recognizes the leader as legitimate
                           and returns to follower state -}
                        convertToFreshFollower raftState remoteTerm peers

            RspAppendEntriesRPC rspAE -> undefined


convertToFreshFollower raftState remoteTerm peers = do
     let updateTerm2Remote = updateTerm remoteTerm
         updateVote2False  = updateVoted False

         newState = ( updateVote2False
                    . updateTerm2Remote ) raftState

     followerService newState peers






send2All ctx peers msg =
    mapM_ (\pid -> sendRaftMsg ctx pid msg) $ peers


heartBeat ctx peers currentTerm leaderId prevLogIndex prevLogTerm entries leaderCommit=
    send2All ctx peers (Rpc $ AppendEntriesRPC $ AppendEntries currentTerm leaderId prevLogIndex prevLogTerm entries leaderCommit)


getTimeout = do
    timeout <- liftIO $ R.randomRIO (150, 300) :: Process Int
    return $ T.after timeout T.Millis


leadrService :: RaftState a b -> [ProcessId] -> Process()
leadrService raftState peers = do
    {- Upon election: send initial empty AppendEntries RPCs
    (heartbeat) to each server; repeat during idle periods to
    prevent election timeouts (§5.2)-}
    say $ "I am leader !!!00"

    --heartBeat ctx peers (current2Remote currentTerm)
    sPid <- getSelfPid
    say $ "I am leader !!! ------- "
    leader raftState (0,0) peers
    return ()

leader ::  RaftState a b -> (Int, Int) -> [ProcessId] -> Process()
leader raftState (nextIndex, matchIndex) peers = do
    let ctx = getContext raftState
        currentTerm = getTerm raftState
    mMsg <- expectMsg ctx-- :: Process RaftMsg

    case mMsg of
        (RaftCommand clientPid c) -> do
            liftIO $ print "GOT MSG"
        --    rs <- stateMachineResp ctx
        --    sendRspToClient ctx clientPid (ClientData rs)

            {--The leader appends the command to its log as a new entry, then
            issues AppendEntries RPCs in parallel to each of the other
            servers to replicate the entry--}


            --send2All ctx peers (Rpc $ AppendEntriesRPC $ AppendEntries currentTerm leaderId prevLogIndex prevLogTerm entries leaderCommit)

            sPid <- getSelfPid
            let newLe = (currentTerm, c)
                newState = addLogEntry newLe raftState
                ae = AppendEntries currentTerm sPid 0 0 ([(currentTerm, c)]) 0

            send2All ctx (filter (/=sPid) peers) (Rpc $ AppendEntriesRPC ae)
            leader raftState (nextIndex, matchIndex) peers

                --where mkAe currentTerm sPid =

        Rpc (RspVoteRPC rs) -> do
            {- If RPC request or response contains term T > currentTerm:
              set currentTerm = T, convert to follower (§5.1-}
            let remoteTerm = termSV rs
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower raftState remoteTerm peers


                termsEqualOrRemoteObsolete ->
                    leader raftState (nextIndex, matchIndex) peers


        Rpc (ReqVoteRPC rv) -> do
            let remoteTerm = termRV rv
            case compareTerms remoteTerm currentTerm of
                CurrentTermObsolete ->
                    convertToFreshFollower raftState remoteTerm peers

                termsEqualOrRemoteObsolete ->
                    leader raftState (nextIndex, matchIndex)  peers


        Rpc (AppendEntriesRPC ae) -> do
            undefined


        Rpc (RspAppendEntriesRPC rspAE) -> undefined




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
