{-# LANGUAGE  TemplateHaskell
, DeriveDataTypeable
, DeriveGeneric
 #-}

module Raft (remoteTable, startRaft) where

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Node
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import Control.Distributed.Process.Extras.Timer as T
import Control.Distributed.Process.Extras.Time as T


import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import qualified Raft.RaftProtocol as Raft


remotable ['Raft.start]

remoteTable :: RemoteTable
remoteTable = Raft.__remoteTable initRemoteTable

startRaftService :: NodeId
                 -> Process ProcessId
startRaftService nid =
    spawn nid ($(mkStaticClosure 'Raft.start))


startRaft :: [LocalNode] -> Process () --[ProcessId]
startRaft raftNodes =
    do peers <- mapM (startRaftService .localNodeId) raftNodes
      -- liftIO $ print (show peers)
       mapM_ (\p -> send p (Raft.Peers peers)) peers
       return ()
