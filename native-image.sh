#!/bin/sh

$GRAALVM_HOME/bin/native-image --verbose -H:ReflectionConfigurationFiles=META-INF/native-image/reflect-config.json \
  --no-fallback --allow-incomplete-classpath  -H:+AddAllCharsets --enable-all-security-services \
  -jar build/libs/rowcp-0.2.0.jar