# Onyx Workshop

These are the material needed to conduct the Onyx workshop. All dependencies are included, no external set up beyond the JVM and Leiningen are needed. Please read all the instructions before beginning!

### Things you need to know

- The workshop is divided into levels and challenges.
- Each level is dedicated to a specific Onyx feature.
- Each level has several challenges that you can work through.
- Every challenge has one test file named `test_<level>_<challenge>.clj` and one implementation file named `challenge_<level>_<challenge>`.
- Every level has a "challenge 0". This challenge is already working, and serves as an example. You don't need to do anything here but read.
- Onyx outputs its log messages to `onyx.log` in this project's root directory. Follow it with `tail -F onyx.log`.
- Exception messages printed to the log are also printed to standard out for convenience.
- All the answers can be found on the `answers` branch of this repository. Check it out and run `lein test`. All the tests should pass.
- This workshop often leaves vars that need to be filled in with values unbound. If you see an exception complaining about an Unbound Var, track it down and fill in the implementation.
- Invoking `lein test` on the master with no changes will cause it to hang. You need to fill in some code to make it run all the way through.

### How to work on a challenge

0. Read the How to Start documentation, listed below, to run the environment sanity check.
1. Open the test file in `test/` for the challenge. Read the comments.
2. Examine the input and expected output.
3. Run the test, watch it fail.
4. Open the corresponding challenge source file in `src/` for the test.
5. Locate the `<<< BEGIN FILL ME IN >>>` and `<<< END FILL ME IN >>>`.
6. Fix the source file with the appropriate changes.
7. Run the test file and pass the test. Refer to Onyx's documentation (links below) and the `answers` branch as needed. I recommend running the tests in a repl to avoid restarting the JVM between test runs. Starting and stopping the Onyx environment only takes about two seconds, so you can iterate much faster.

Remember that challenge 0 on every level is already working!

### How to start

Begin the workshop with level 0, challenge 0. This is a test to ensure that your environment is sane and working. If all is well, you should see the following on standard out:

```text
Starting Onyx development environment
==== Start job output ====
[{:sentence "Getting started with Onyx is easy"}
 {:sentence "This is a segment"}
 {:sentence "Segments are Clojure maps"}
 {:sentence "Sample inputs are easy to fabricate"}
 :done]
==== End job output ====
Stopping Onyx development environment
```

Once you see this, proceed to level 1, challenge 0. Run the test and observe the already-working example in challenge 0, then proceed to the next challenge. Once you finish all the challenges, proceed to the next level and repeat.

### Reading

Each file has a moderate amount of comments to frame the discussion. Still, here are some links that will be helpful if you're approaching Onyx from scratch or need more information to help solve a challenge:

- [Basic terminology and concepts](http://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/concepts.html)
- [Full User Guide](http://onyx-platform.gitbooks.io/onyx/content/)
- [Examples Repository](https://github.com/onyx-platform/onyx-examples)

### Gotchas

- Some of the tests capture standard out using `clojure.core/with-out-str` to make verifying assertions as easy as possible. If you're wondering where your printlns are going, check the test harness for the challenge. Feel free to drop it while you debug. 
- If your environment starts and then hangs on the Onyx log line `INFO [onyx.log.zookeeper] - Starting ZooKeeper server`, your ZooKeeper port is in use. Switch the port to an unused port in `workshop.workshop-utils/zk-port`.

### Guide

Below is the table of contents for the levels and challenges you'll be working through. Feel free to skip around.

- Level 0: Sanity Check
  - Challenge 0: Makes sure your environment is working okay
- Level 1: Workflows
  - Challenge 0: Observe a minimal workflow
  - Challenge 1: Implement a linear workflow
  - Challenge 2: Implement a branched workflow
  - Challenge 3: Implement a larger DAG workflow
- Level 2: Catalogs
  - Challenge 0: Observe a minimal catalog
  - Challenge 1: Implement a catalog entry for a function
  - Challenge 2: Parameterize a function through the catalog
  - Challenge 3: Implement a grouping task
  - Challenge 4: Bound the number of peers executing a task
- Level 3: Functions
  - Challenge 0: Observe a basic function
  - Challenge 1: Implement 3 task functions
  - Challenge 2: Write a function that emits multiple segments
  - Challenge 3: Write a catalog entry and parameterized function
  - Challenge 4: Implement a bulk function
- Level 4: Lifecycles
  - Challenge 0: Observe a lifecycle example
  - Challenge 1: Use lifecycles for logging
  - Challenge 2: Compute an aggregate
  - Challenge 3: Parameterize a function with state
- Level 5: Flow Conditions
  - Challenge 0: Observe a flow conditions example
  - Challenge 1: Use flow conditions for basic routing
  - Challenge 2: Create a composite flow conditions predicate
  - Challenge 3: Write an exception handler with flow conditions
  - Challenge 4: Use key exclusion

## License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
