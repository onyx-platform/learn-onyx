# lambdajam-2015

Onyx workshop materials for the LambdaJam 2015 conference. This workshop assumes attendees were at the Onyx talk earlier in the day and are familiar with the basic concepts.

### How this works

This repository contains a set of tests that can be invoked with `lein test`. The workshop is divided into levels and challenges. Each level is dedicated to a particular Onyx feature. Each level has several challenges that you can work through. Every challenge is self contained in its own test (named `test_<level>_<challenge>.clj` and implementation file (named `challenge_<level>_<challenge>`). Every level has a "challenge 0". This challenge is already-working, and is a basic example of how to use the feature under discussion to help you get started.

### Guide

- Level 0: Sanity Check
  - Challenge 0: Makes sure your environment is working okay
- Level 1: Workflows
  - Challenge 0: Observe a minimal workflow
  - Challenge 1: Implement a linear workflow
  - Challenge 2: Implement a branched workflow
  - Challenge 3: Implement a larger DAG workflow
  - Challenge 4: Trigger a workflow task not-found error
- Level 2: Catalogs
  - Challenge 0: Observe a minimal catalog
  - Challenge 1: Implement a catalog entry for a function
  - Challenge 2: Parameterize a function through the catalog
  - Challenge 3: Implement a stateful grouping task
  - Challenge 4: Bound the number of peers executing a task
  - Challenge 5: Trigger a function not-found error
- Level 3: Functions
- Level 4: Flow Conditions
- Level 5: Lifecycles

## License

Copyright © 2015 Michael Drogalis

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
