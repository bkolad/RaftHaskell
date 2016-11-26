{-# LANGUAGE  TemplateHaskell
, DeriveDataTypeable
, DeriveGeneric
, RankNTypes
, ExistentialQuantification
, MultiParamTypeClasses
, FlexibleInstances
, FunctionalDependencies
, TemplateHaskell
, FlexibleContexts
 #-}

module Raft.Types
    ( module Control.Distributed.Process
    , module Raft.Types
    , module R
    , module Data.Typeable
    , module Data.Binary
    ) where

import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import Control.Distributed.Process
import qualified System.Random as R
import Data.Typeable
import Control.Lens hiding (Context)



data Peers = Peers ![ProcessId]
    deriving (Eq, Show, Typeable, Generic)

instance Binary Peers


data Current
data Remote


data RequestVote = RequestVote
          { termRV         :: !Term
          , candidateIdRV  :: !ProcessId
          , lastLogIndexRV :: !Int
          , lastLogTermRV  :: !Int
          }
          deriving (Eq, Show, Typeable, Generic)


instance Binary RequestVote


data RspVote = RspVote
              { termSV        :: !Term
              , voteGrantedSV :: !Bool
              }
              deriving (Eq, Show, Typeable, Generic)


instance Binary RspVote


data AppendEntries a = AppendEntries
    { termAE       :: !Term
    , leaderId     :: !ProcessId
    , prevLogIndex :: !Int
    , prevLogTerm  :: !Term
    , entries      :: ![(Term, a)] --log entries to store (Nothing for heartbeat;
    , leaderCommit :: !Int
    }
    deriving (Eq, Show, Typeable, Generic)

instance Binary a => Binary (AppendEntries a)


data RspAppendEntries = RspAppendEntries
    { termRAE :: Int
    , success :: Bool
    }
    deriving (Eq, Show, Typeable, Generic)


instance Binary RspAppendEntries


data RpcMsg a = ReqVoteRPC RequestVote
              | RspVoteRPC RspVote
              | AppendEntriesRPC (AppendEntries a)
              | RspAppendEntriesRPC RspAppendEntries
              deriving (Eq, Show, Typeable, Generic)


instance Binary a => Binary (RpcMsg a)


data Command = SET Int | GET
    deriving (Eq, Show, Typeable, Generic)

instance Binary Command


data CommandRsp = Val Int
    deriving (Eq, Show, Typeable, Generic)

instance Binary CommandRsp

data ClientData a = ClientData a
                    | LastKonownLeaderId ProcessId
                    | LeaderRecived
    deriving (Eq, Show, Typeable, Generic)

instance Binary a => Binary (ClientData a)



data RaftMsg a = RaftCommand ProcessId a | Rpc (RpcMsg a)
        deriving (Eq, Show, Typeable, Generic)


instance Binary a => Binary (RaftMsg a)


data Context a b = Context
             { sendRaftMsg :: ProcessId -> RaftMsg a -> Process()
             , expectMsgTimeout :: Int -> Process (Maybe (RaftMsg a))
             , expectMsg :: Process (RaftMsg a)
             , sendRspToClient :: ProcessId -> ClientData b -> Process ()
             , stateMachineResp :: Process b
             }


mkContext :: (Typeable a, Binary a, Typeable b, Binary b) => Process b -> Context a b
mkContext stMachine =  Context
    { sendRaftMsg      = send
    , expectMsgTimeout = expectTimeout
    , expectMsg        = expect
    , sendRspToClient  = send
    , stateMachineResp = stMachine
    }

context :: Context Command CommandRsp
context = mkContext (return $ Val 99)



data TermComparator = RemoteTermObsolete
                    | TermsEqual
                    | CurrentTermObsolete


compareTerms :: Term -> Term -> TermComparator
compareTerms remoteTerm currentTerm
    | currentTerm >  remoteTerm = RemoteTermObsolete
    | currentTerm == remoteTerm = TermsEqual
    | currentTerm <  remoteTerm = CurrentTermObsolete


type Term = Int
type RaftPersistentState a = (Term, Bool, [(Term, a)])

type CommitIndex = Int
type LastApplied = Int
type RaftVolatileState = (CommitIndex, LastApplied)



data RaftState a b = RaftState
    { _persistentStateRS :: RaftPersistentState a
    , _volatileStateRS   :: RaftVolatileState
    , _contextRS         :: Context a b
    }

makeLenses ''RaftState

getTerm rs = rs ^. (persistentStateRS . _1)

getVoted rs = rs ^. (persistentStateRS . _2)

updateVoted b rs =  set (persistentStateRS . _2) b rs

updateTerm t rs = set (persistentStateRS . _1) t rs

updateLogEntries le rs = set (persistentStateRS . _3) le rs

addLogEntry e rs = over (persistentStateRS . _3) (\ls -> e:ls) rs

getContext rs = rs ^. contextRS

getCommitIndex rs = rs ^. (volatileStateRS . _1)

getLastApplied rs = rs ^. (volatileStateRS . _2)
