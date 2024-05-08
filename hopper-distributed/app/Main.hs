module Main where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Concurrently (..))
import qualified Hopper.Distributed.Executor
import qualified Hopper.Distributed.Scheduler
import Hopper.Distributed.Scheduler.Trace (nullTracer)
import qualified Hopper.Scheduler
import System.Random (randomIO, randomRIO)

main :: IO ()
main = do
  args <- getArgs
  if args == ["executor"]
    then do
      Hopper.Distributed.Executor.run "localhost" 4000 $ \taskId task -> do
        shouldFail <- randomIO

        when shouldFail $
          error "this one failed"

        print (taskId, task)
        pure "done"
    else do
      let 
        handleLostTask :: 
          [Hopper.Scheduler.Task Hopper.Distributed.Scheduler.Task] ->
          Hopper.Scheduler.Reason -> 
          IO ()
        handleLostTask task reason = 
           print ("Lost" :: Text, task, reason)

      let 
        handleTaskResult :: ByteString ->
          Either Hopper.Scheduler.TaskExecutionError ByteString ->
          IO ()
        handleTaskResult taskId taskResult = 
            print ("coolio" :: Text, taskId, taskResult)

      Hopper.Scheduler.withScheduler handleLostTask handleTaskResult $ \scheduler -> do
        void $
          runConcurrently $
            (,)
              <$> ( Concurrently $
                      forever $ do
                        delay <- randomRIO (1, 10)
                        threadDelay (delay * 1000)

                        id' <- randomRIO (0 :: Int, 100000000)
                        let id = encodeUtf8 (show @Text id')

                        print ("scheduling task" :: Text, id)

                        Hopper.Scheduler.scheduleTask
                          scheduler
                          id
                          (Hopper.Distributed.Scheduler.Task "cool task" (Just "node 2"))
                          Nothing
                  )
              <*> ( Concurrently $
                      Hopper.Distributed.Scheduler.run nullTracer (Just "node 1") scheduler
                  )
