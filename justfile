set shell := ["bash", "-uc"]

# Just echo the purpose of this file
_default:
    @echo "This file is used to automate some release tasks"
    @echo "(running in `pwd`)"
    @just --list

# Build locally without tests
build:
    @echo "Building locally without tests"
    ./mvnw clean install -DskipTests -T1C

# Build locally with tests
test:
    @echo "Testing locally"
    ./mvnw clean verify

# Build on CI without tests
build-ci:
    ./mvnw -B -ntp -s .build/ci-maven-settings.xml clean install -DskipTests

# Test on CI with tests
test-ci:
    ./mvnw -B -ntp -s .build/ci-maven-settings.xml clean verify

# Perform a release
perform-release: pre-release release post-release
    @echo "🎉 Successfully released Smallrye Reactive Messaging ${RELEASE_VERSION} 🚀"

# Decrypt secrets
decrypt-secrets:
    @if [[ -z "${SECRET_FILES_PASSPHRASE}" ]]; then exit 1; fi
    @echo "🔐 Decrypting smallrye signature"
    gpg --quiet --batch --yes --decrypt --passphrase="${SECRET_FILES_PASSPHRASE}" \
        --output smallrye-sign.asc .build/smallrye-sign.asc.gpg
    @echo "🔐 Decrypting Maven settings"
    gpg --quiet --batch --yes --decrypt --passphrase="${SECRET_FILES_PASSPHRASE}" \
        --output maven-settings.xml .build/maven-settings.xml.gpg

# Initialize GnuPG
init-gpg:
    @echo "🔐 GnuPG setup"
    gpg --fast-import --no-tty --batch --yes smallrye-sign.asc

# Initialize Git
init-git:
    @echo "🔀 Git setup"
    git config --global user.name "smallrye-ci"
    git config --global user.email "smallrye@googlegroups.com"

# Steps before releasing
pre-release: decrypt-secrets init-gpg init-git
    @echo "🚀 Pre-release steps..."
    @if [[ -z "${RELEASE_TOKEN}" ]]; then exit 1; fi
    @if [[ -z "${RELEASE_VERSION}" ]]; then exit 1; fi
    @echo "Pre-release verifications"
    jbang .build/PreRelease.java --token=${RELEASE_TOKEN} --release-version=${RELEASE_VERSION}
    @echo "Bump project version to ${RELEASE_VERSION}"
    ./mvnw -B -ntp versions:set -DnewVersion=${RELEASE_VERSION} -DgenerateBackupPoms=false -s .build/ci-maven-settings.xml
    @echo "Check that the project builds (no tests)"
    ./mvnw -B -ntp clean install -Prelease -DskipTests -s maven-settings.xml
    @echo "Check that the website builds"
    -[[ ${DEPLOY_WEBSITE} == "true" ]] && cd documentation && pipenv install && pipenv run mkdocs build

# Steps to release
release: pre-release
    @echo "🚀 Release steps..."
    @if [[ -z "${JRELEASER_TAG_NAME}" ]]; then exit 1; fi
    @if [[ -z "${JRELEASER_PREVIOUS_TAG_NAME}" ]]; then exit 1; fi
    @if [[ -z "${JRELEASER_GITHUB_TOKEN}" ]]; then exit 1; fi
    @echo "Commit release version and push upstream"
    git commit -am "[RELEASE] - Bump version to ${RELEASE_VERSION}"
    git push
    jbang .build/CompatibilityUtils.java extract
    @echo "Call JReleaser"
    ./mvnw -B -ntp jreleaser:full-release -Pjreleaser -pl :smallrye-reactive-messaging -s .build/ci-maven-settings.xml
    @echo "Bump to 999-SNAPSHOT and push upstream"
    ./mvnw -B -ntp versions:set -DnewVersion=999-SNAPSHOT -DgenerateBackupPoms=false -s .build/ci-maven-settings.xml
    git commit -am "[RELEASE] - Next development version: 999-SNAPSHOT"
    git push

# Deploy to Maven Central
deploy-to-maven-central: decrypt-secrets init-gpg
    @echo "🔖 Deploy to Maven Central"
    ./mvnw -B -ntp deploy -Prelease -DskipTests -s maven-settings.xml

# Steps post-release
post-release:
    @echo "🚀 Post-release steps..."
    -[[ ${CLEAR_REVAPI} == "true" ]] && just clear-revapi
    -[[ ${DEPLOY_WEBSITE} == "true" ]] && just deploy-docs

# Update Pulsar Connector Configuration Documentation
update-pulsar-config-docs:
    @echo "📝 Updating Pulsar connector configuration docs"
    jbang .build/PulsarConfigDoc.java -d documentation/src/main/docs/pulsar/config

# Deploy documentation
deploy-docs:
    @echo "📝 Deploying documentation to GitHub"
    @if [[ -z "${RELEASE_VERSION}" ]]; then exit 1; fi
    ./mvnw -B -ntp clean compile -f documentation && cd documentation && pipenv install && pipenv run mike deploy --update-aliases --push --remote origin "${RELEASE_VERSION}" "latest"

# Clear RevAPI justifications
clear-revapi:
    #!/usr/bin/env bash
    jbang .build/CompatibilityUtils.java clear
    if [[ $(git diff --stat) != '' ]]; then
      git add -A
      git status
      git commit -m "[POST-RELEASE] - Clearing breaking change justifications"
      git push
    else
      echo "No justifications cleared"
    fi
