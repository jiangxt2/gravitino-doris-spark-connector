/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.jiangxt2.gravitino.doris.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AbstractConfiguration;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/** A scoped, thread-safe Log4j appender for credential-safety assertions in tests. */
public final class InMemoryLogCapture implements AutoCloseable {

  private static final String COMPLETE_EVENT_PATTERN =
      "[%t] %-5level %logger %marker %X %x - %msg%n%throwable";

  private final LoggerContext loggerContext;
  private final AbstractConfiguration configuration;
  private final LoggerConfig loggerConfig;
  private final String loggerName;
  private final Level originalLevel;
  private final boolean ownsLoggerConfig;
  private final CaptureAppender appender;
  private final AtomicBoolean closed = new AtomicBoolean();

  private InMemoryLogCapture(String loggerName, Level captureLevel) {
    loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration currentConfiguration = loggerContext.getConfiguration();
    configuration = (AbstractConfiguration) currentConfiguration;
    this.loggerName = loggerName;
    LoggerConfig exactLoggerConfig = currentConfiguration.getLoggers().get(loggerName);
    if (captureLevel != null && exactLoggerConfig == null) {
      loggerConfig = new LoggerConfig(loggerName, captureLevel, true);
      originalLevel = null;
      ownsLoggerConfig = true;
      configuration.addLogger(loggerName, loggerConfig);
    } else {
      loggerConfig =
          exactLoggerConfig == null
              ? currentConfiguration.getLoggerConfig(loggerName)
              : exactLoggerConfig;
      originalLevel = captureLevel == null ? null : loggerConfig.getLevel();
      ownsLoggerConfig = false;
      if (captureLevel != null) {
        loggerConfig.setLevel(captureLevel);
      }
    }
    appender =
        new CaptureAppender(
            "in-memory-log-capture-" + System.identityHashCode(this), currentConfiguration);
    appender.start();
    currentConfiguration.addAppender(appender);
    loggerConfig.addAppender(appender, null, null);
    loggerContext.updateLoggers();
  }

  /** Starts capturing events handled by the configured logger or its nearest configured parent. */
  public static InMemoryLogCapture start(String loggerName) {
    return new InMemoryLogCapture(loggerName, null);
  }

  /** Starts capturing the named logger at the supplied level without changing unrelated loggers. */
  public static InMemoryLogCapture start(String loggerName, Level captureLevel) {
    return new InMemoryLogCapture(loggerName, captureLevel);
  }

  /**
   * Returns immutable rendered events, including marker, context data, context stack, and
   * throwable.
   */
  public List<String> renderedEvents() {
    return appender.events.stream()
        .map(appender.layout::toSerializable)
        .collect(Collectors.toUnmodifiableList());
  }

  /** Returns immutable thread names associated with the captured events. */
  public List<String> threadNames() {
    return appender.events.stream()
        .map(LogEvent::getThreadName)
        .collect(Collectors.toUnmodifiableList());
  }

  /** Removes and stops the scoped appender. */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    loggerConfig.removeAppender(appender.getName());
    if (ownsLoggerConfig) {
      configuration.removeLogger(loggerName);
    } else if (originalLevel != null) {
      loggerConfig.setLevel(originalLevel);
    }
    appender.stop();
    configuration.removeAppender(appender.getName());
    loggerContext.updateLoggers();
  }

  private static final class CaptureAppender extends AbstractAppender {
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();
    private final PatternLayout layout;

    private CaptureAppender(String name, Configuration configuration) {
      this(
          name,
          PatternLayout.newBuilder()
              .withConfiguration(configuration)
              .withPattern(COMPLETE_EVENT_PATTERN)
              .withAlwaysWriteExceptions(true)
              .build());
    }

    private CaptureAppender(String name, PatternLayout layout) {
      super(name, null, layout, true, null);
      this.layout = layout;
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }
  }
}
