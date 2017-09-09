# Contribution Guide
dble is a community driven open source project and we welcome any contributor.This guide documents the best way to make various types of contribution to dble, including what is required before submitting a code change.  
Of course, contributing doesn't just mean writing code. Improving documentation and reporting bugs are also welcome.

## Contributing Bug Reports  
Ideally, bug reports are accompanied by a proposed code change to fix the bug. This isn’t always possible, as those who discover a bug may not have the experience to fix it. A bug may be reported but without creating a pull request (see below).

Bug reports are only useful however if they include enough information to understand, isolate and ideally reproduce the bug.Unreproducible bugs, or simple error reports, may be closed.

It is possible to propose new features as well. These are generally not helpful unless accompanied by detail, such as a design document and/or code change. Feature requests may be rejected, or closed after a long period of inactivity.

## Contributing Documentation Changes
To propose a change to documentation, similarly, edit the Markdown file in the repository and open a pull request.

## Contributing Code Changes
This part outlines some conventions about development workflow, commit message formatting, contact points and other resources to make it easier to get your contribution accepted. 

## Preparing to Contribute Code Changes
### Choosing What to Contribute

Before you move on, please make sure what your issue and/or pull request is, a bug fix or an architecture change.

You know that review can take hours or days of committer time, so
everyone benefits if contributors focus on changes that are useful, clear, easy to evaluate, and already pass basic checks.

Besides all above, each issue should be filed with template.


### Is this a bug fix?

Bug fixes usually come with tests. With the help of continuous integration test, patches can be easy to review. Please update the unit tests so that they catch the bug!  
IMPORTANT:Issue example will be comming soon.

### Is this an architecture improvement?

Some examples of "Architecture" improvements:

- Improving test coverage.
- Decoupling logic or creation of new utilities.
- Making code more resilient (sleeps, back offs, reducing flakiness, etc).
- Improving performance.


If you are improving the quality of code, then justify/state exactly what you are 'cleaning up' in your Pull Request so as to save reviewers' time.   
IMPORTANT: An example will be comming soon.

If you're making code more resilient, test it locally to demonstrate how exactly your patch changes things.


### Workflow

### Step 1: Fork in the cloud

1. Visit https://github.com/actiontech/dble
2. Click `Fork` button (top right) to establish a cloud-based fork.

### Step 2: Clone fork to local storage


Create your clone:


```
mkdir -p $working_dir
cd $working_dir
git clone git@github.com:$user/dble.git
# the following is recommended
# or: git clone https://github.com/$user/dble.git 

cd $working_dir/dble
git remote add upstream git@github.com:actiontech/dble.git
# or:git remote add upstream https://github.com/actiontech/dble.git

# Never push to upstream master since you do not have write access
git remote set-url --push upstream no_push

# Confirm that your remotes make sense:
# It should look like:
# origin    git@github.com:$(user)/dble.git (fetch)
# origin    git@github.com:$(user)/dble.git (push)
# upstream  https://github.com/actiontech/dble (fetch)
# upstream  no_push (push)
git remote -v
```


### Step 3: Branch

Get your local master up to date:

```sh
cd $working_dir/dble
git fetch upstream
git checkout master
git rebase upstream/master
```

Branch from master:

```sh
git checkout -b yours-issue
```

Then edit code on the `yours-issue` branch.

#### Compile, Check, Test

```sh
# Complie and run unit test to make sure all test passed
mvn clean install package 
```

### Step 4: Keep your branch in sync

```sh
# While on your yours-issue branch
git fetch upstream
git rebase upstream/master
```

### Step 5: Commit

Commit your changes.

```sh
git commit
```

Likely you'll go back and edit/build/test some more than `commit --amend`
in a few cycles.

### Step 6: Push

When ready to review (or just to establish an off-site backup or your work),
push your branch to your fork on `github.com`:

```sh
git push -f origin yours-issue
```

### Step 7: Create a pull request

1. Visit your fork at https://github.com/$user/dble (replace `$user` obviously).
2. Click the `Compare & pull request` button next to your `yours-issue` branch.

#### Step 8: get a code review

Once your pull request has been opened, it will be assigned to at least one
reviewers. Those reviewers will do a thorough code review, looking for
correctness, bugs, opportunities for improvement, documentation and comments,
and style.

Commit changes made in response to review comments to the same branch on your
fork.

Very small PRs are easy to review. Very large PRs are very difficult to
review.

## Commit message style

Please follow this style to make dble easy to review, maintain and develop.

```
<what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<footer>(optional)
```

The first line is the subject and should be no longer than 70 characters, the
second line is always blank, and other lines should be wrapped at 80 characters.
This allows the message to be easier to read on GitHub as well as in various
git tools.

For the why part, if no specific reason for the change,
you can use one of some generic reasons like "Improve documentation.",
"Improve performance.", "Improve robustness.", "Improve test coverage."

