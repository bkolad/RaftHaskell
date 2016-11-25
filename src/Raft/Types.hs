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

-- Phantom Type
data Term a = Term !Int
    deriving (Eq, Show, Typeable, Generic)

remmote2Current :: Term Remote -> Term Current
remmote2Current (Term t) = Term t

current2Remote :: Term Current -> Term Remote
current2Remote (Term t) = Term t


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


data AppendEntries a = AppendEntries
    { termAE       :: !(Term Remote) -- TODO refactor term
    , leaderId     :: !ProcessId
    , prevLogIndex :: !Int
    , prevLogTerm  :: !(Term Remote)
    , entries      :: !(Maybe [(Term Remote, a)]) --log entries to store (Nothing for heartbeat;
    , leaderCommit :: !Int
    }
    deriving (Eq, Show, Typeable, Generic)

instance Binary a => Binary (AppendEntries a)


data RpcMsg a = ReqVoteRPC RequestVote
            | RspVoteRPC RspVote
            | AppendEntriesRPC (AppendEntries a)
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


data Context a b= Context
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


compareTerms :: Term Remote -> Term Current -> TermComparator
compareTerms (Term remoteTerm) (Term currentTerm)
    | currentTerm >  remoteTerm = RemoteTermObsolete
    | currentTerm == remoteTerm = TermsEqual
    | currentTerm <  remoteTerm = CurrentTermObsolete

type RaftPersistentState a = (Term Current, Bool, [a])
type RaftVolatileState = (Int, Int)



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

getContext rs = rs ^. contextRS
