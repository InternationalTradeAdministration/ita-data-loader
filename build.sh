#!/usr/bin/env bash
node -v
npm -v
cd client && npm ci && npm run build
cd ..
rm -rf src/main/resources/public
cp -r client/dist src/main/resources/public
./gradlew buildJarAndCopyToDocker
