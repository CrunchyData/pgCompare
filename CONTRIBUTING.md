# Contributing to ConferoDC

ConferoDC is released under the Apache 2.0 license. If you would like to contribute something, or want to hack on the code this document should help you get started. If you would like to contribute code you can do so through GitHub by forking the repository and sending a pull request targeting the main branch.

## Using GitHub Issues
We use GitHub issues to track bugs and enhancements.

If you are reporting a bug, please help to speed up problem diagnosis by providing as much information as possible.
Ideally, that would include steps that reproduces the problem.

## How to Contribute
Thank you so much for wanting to contribute! Unless the change is a trivial fix such as for a typo, it's generally best to start by opening a new issue describing the bug or feature you're intending to fix. Even if you think it's relatively minor, it's helpful to know what people are working on. And as mentioned above, API changes should be discussed thoroughly before moving to code.

All contributions must be licensed Apache 2.0 and all files must have a copy of the boilerplate license comment (can be copied from an existing file).

Here are a few important things you should know about contributing:

- Pull requests are great for small fixes for bugs, documentation, etc.
- Pull requests are not merged directly into the main branch.

Some examples of types of pull requests that are immediately helpful:

- Fixing or improving documentation.
- Improvements to Maven configuration.


## Labels
Labels on issues are managed by contributors, you don't have to worry about them. Here's a list of what they mean:

- bug: feature that should work, but doesn't
- enhancement: minor tweak/addition to existing behavior
- feature: new behavior, bigger than enhancement
- reproducible: has enough information to very easily reproduce
- duplicate: there's another issue which already covers/tracks this
- invalid: there isn't enough information to make a verdict, or unrelated

## License

By contributing your code, you agree to license your contribution under the terms of the APLv2.

All files are released with the Apache 2.0 license.

If you are adding a new file it should have a header like this:

/*
 * Copyright 2012-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


## Code Conventions
When submitting code, please make every effort to follow existing conventions and style in order to keep the code as readable as possible.

- Make sure all new `.java` files have a Javadoc class comment with at least an `@author` tag identifying you, and preferably at least a paragraph on what the class is for.
- Add the ASF license header comment to all new .java files (copy from existing files in the project).
