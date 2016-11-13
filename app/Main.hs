module Main where

import Control.Distributed.Process.Extras.Time as T
import Control.Distributed.Process.Node (LocalNode, runProcess, forkProcess)
import Control.Distributed.Process -- (Process)


import Raft (remoteTable, startRaft)
import Network.NodeManager (getClientNode, getRaftNodes)

import Control.Distributed.Process.Backend.SimpleLocalnet
  {--( Backend
  , redirectLogsHere
  , startSlave
  , initializeBackend
  , findSlaves
  , newLocalNode
)--}

import Control.Concurrent

main :: IO ()
main = do
    raftNodes  <- getRaftNodes remoteTable
    clientNode <- getClientNode remoteTable
    runProcess clientNode (startRaft raftNodes)
    return ()
