module Raft.Client where
import Raft.Types

import qualified System.Random as R


clientService :: [ProcessId] -> Process()
clientService  peers = do
    peer <- liftIO $ randomPeer peers
    sPid <- getSelfPid
    let command = RaftCommand sPid (SET 4)

    send peer command
    client peers peer command 1000000
    --send peer x
    {--mA <- expectTimeout 1000 :: Process (Maybe CommandData)
    case mA of
        Nothing ->
            clientService ps
        Just a ->
            client a
--}

randomPeer peers = do
    let pSize = length peers
    r <- R.randomRIO (0, pSize -1 )
    return $ peers !! r




client peers peer command timeout = do
    m <- expectTimeout timeout :: Process (Maybe (ClientData CommandRsp))
    case m of
        Nothing -> do
            liftIO $ print "TOUT"
            newPeer <- liftIO $ randomPeer peers
            send peer command
            client peers newPeer command timeout

        Just (LastKonownLeaderId pid) -> do
            liftIO $ print "RSP"
            client peers pid command timeout

        Just LeaderRecived -> return ()
        Just (ClientData (Val x)) -> do
            liftIO $ print $ show x




    return ()
