module Network.NodeManager
    ( getClientNode
    , getRaftNodes
    )where


import Control.Distributed.Process.Node
import Network.Transport.TCP (createTransport, defaultTCPParameters)
import Control.Distributed.Process



clientConf = ("localhost", "44444")


raftsNodeConf = [ ("localhost", "44445")
                , ("localhost", "44446")
                , ("localhost", "44447")
                , ("localhost", "44448")
                , ("localhost", "44449")
                , ("localhost", "44450")
                , ("localhost", "44451")
                , ("localhost", "44452")
                , ("localhost", "44453")
                , ("localhost", "44454")
                , ("localhost", "44455")
                ]



getClientNode :: RemoteTable
              -> IO LocalNode
getClientNode remoteTable =
    head <$> getNodes [clientConf] remoteTable -- safe head
                                               -- (one element list)


getRaftNodes :: RemoteTable
             -> IO [LocalNode]
getRaftNodes remoteTable = getNodes raftsNodeConf remoteTable

getNodes :: [(String, String)]
          -> RemoteTable
          -> IO [LocalNode]
getNodes conf remoteTable  = do
    trnasports <- traverse getTransport conf
    let transportLs = sequence trnasports

    case transportLs of
        Left l ->
            error $ "Problem with creating transport " ++ show l

        Right ts ->
            traverse (\t -> newLocalNode t remoteTable) ts

    where
        getTransport (host, port) =
            createTransport host port defaultTCPParameters
