module Hopper.Distributed.ThriftClient
  ( Client,
    newClient,
    call,
  )
where

import Data.Streaming.Network (getSocketTCP)
import Pinch.Client (Client, call, client, createChannel)
import Pinch.Protocol.Compact (compactProtocol)
import Pinch.Transport (framedTransport)

newClient ::
  ByteString ->
  Int ->
  IO Client
newClient host port = do
  (socket, _sockAddr) <- getSocketTCP host port
  channel <- createChannel socket framedTransport compactProtocol
  pure (client channel)
