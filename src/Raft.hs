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
import Raft.Types

import Data.Binary
import Data.Typeable (Typeable)
import GHC.Generics (Generic)

import qualified Raft.RaftProtocol as Raft
import Raft.Client

remotable ['Raft.start]

remoteTable :: RemoteTable
remoteTable = Raft.__remoteTable initRemoteTable

startRaftService :: NodeId
                 -> Process ProcessId
startRaftService nid =
    spawn nid ($(mkStaticClosure 'Raft.start))



observer peers = do
    mapM_ monitor peers
    go
    where
        go = do
            ProcessMonitorNotification _ref deadpid reason <- expect
            let msg = show deadpid ++ " " ++ show reason
            say msg
            go


startRaft :: [LocalNode] -> Process () --[ProcessId]
startRaft raftNodes =
    do peers <- mapM (startRaftService .localNodeId) raftNodes
       spawnLocal (observer peers)

      -- liftIO $ print (show peers)
       mapM_ (\p -> send p (Peers peers)) peers

       spawnLocal (clientService peers)

       return ()
