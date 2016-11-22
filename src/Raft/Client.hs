module Raft.Client where
import Raft.Types

import qualified System.Random as R


clientService :: Peers -> Process()
clientService ps@(Peers peers) = do
    let pSize = length peers
    r <- liftIO $ R.randomRIO (0, pSize -1 )-- :: Process Int
    let peer = peers !! r
    let x = 33 :: Int
    send peer x
    mA <- expectTimeout 1000 :: Process (Maybe CommandData)
    case mA of
        Nothing ->
            clientService ps
        Just a ->
            client a





client peer = undefined
