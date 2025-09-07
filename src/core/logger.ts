/**
 * Logger Interface - Configurable logging for RedlockToolkit
 * Allows users to provide their own logger implementation
 */

export enum LogLevel {
  ERROR = 0,
  WARN = 1,
  INFO = 2,
  DEBUG = 3,
  TRACE = 4
}

export interface LogContext {
  [key: string]: unknown;
}

export interface Logger {
  error(message: string, context?: LogContext): void;
  warn(message: string, context?: LogContext): void;
  info(message: string, context?: LogContext): void;
  debug(message: string, context?: LogContext): void;
  trace(message: string, context?: LogContext): void;
}

/**
 * Default console logger implementation
 */
export class ConsoleLogger implements Logger {
  constructor(private level: LogLevel = LogLevel.INFO) {}

  error(message: string, context?: LogContext): void {
    if (this.level >= LogLevel.ERROR) {
      console.error(message, context || "");
    }
  }

  warn(message: string, context?: LogContext): void {
    if (this.level >= LogLevel.WARN) {
      console.warn(message, context || "");
    }
  }

  info(message: string, context?: LogContext): void {
    if (this.level >= LogLevel.INFO) {
      console.info(message, context || "");
    }
  }

  debug(message: string, context?: LogContext): void {
    if (this.level >= LogLevel.DEBUG) {
      console.debug(message, context || "");
    }
  }

  trace(message: string, context?: LogContext): void {
    if (this.level >= LogLevel.TRACE) {
      console.trace(message, context || "");
    }
  }
}

/**
 * Silent logger that discards all messages
 */
export class SilentLogger implements Logger {
  error(): void {
    // Silent
  }

  warn(): void {
    // Silent
  }

  info(): void {
    // Silent
  }

  debug(): void {
    // Silent
  }

  trace(): void {
    // Silent
  }
}

/**
 * Logger factory for creating appropriate logger instances
 */
export class LoggerFactory {
  static create(logger?: Logger | boolean): Logger {
    if (logger === false) {
      return new SilentLogger();
    }
    
    if (logger === true) {
      return new ConsoleLogger();
    }
    
    if (logger) {
      return logger;
    }
    
    // Default to silent logger for library
    return new SilentLogger();
  }
}