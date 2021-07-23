package com.risingwave.planner.handler;

import com.google.common.collect.ImmutableMap;
import com.risingwave.common.error.ExecutionError;
import com.risingwave.common.exception.RisingWaveException;
import com.risingwave.planner.context.ExecutionContext;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factor for creating sql handler.
 *
 * <p>When loaded, this class will scan classpath for {@link SqlHandler} implementations.
 *
 * <p>A {@link SqlHandler} implementation has following requirements:
 *
 * <ul>
 *   <li>Must lie in package {@link com.risingwave.planner.handler}
 *   <li>A public constructor without argument.
 *   <li>Annotated with {@link HandlerSignature} to provides sql kinds it can handle
 *   <li>Implements {@link SqlHandler}.
 * </ul>
 *
 * @see CreateTableHandler
 */
public class SqlHandlerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlHandlerFactory.class);
  private static final String PACKAGE_NAME = SqlHandlerFactory.class.getPackage().getName();

  private static final ImmutableMap<SqlKind, Constructor<? extends SqlHandler>>
      SQL_HANDLER_FACTORY = createSqlHandlerFactory();

  public static SqlHandler create(SqlNode ast, ExecutionContext context) {
    Constructor<? extends SqlHandler> constructor = SQL_HANDLER_FACTORY.get(ast.getKind());

    if (constructor == null) {
      throw RisingWaveException.from(ExecutionError.NOT_IMPLEMENTED, ast.getKind());
    }

    try {
      return constructor.newInstance();
    } catch (Exception e) {
      LOGGER
          .atError()
          .addArgument(ast.getKind())
          .addArgument(e)
          .log("Failed to create handler for {}");
      throw RisingWaveException.from(ExecutionError.INTERNAL, e);
    }
  }

  private static ImmutableMap<SqlKind, Constructor<? extends SqlHandler>>
      createSqlHandlerFactory() {
    Reflections reflections = new Reflections(new ConfigurationBuilder().forPackages(PACKAGE_NAME));

    ImmutableMap.Builder<SqlKind, Constructor<? extends SqlHandler>> builder =
        ImmutableMap.builder();

    for (Class<? extends SqlHandler> klass : reflections.getSubTypesOf(SqlHandler.class)) {

      HandlerSignature handlerSignature = klass.getAnnotation(HandlerSignature.class);
      if (handlerSignature != null) {
        try {
          Constructor<? extends SqlHandler> emptyConstructor = klass.getDeclaredConstructor();
          Arrays.stream(handlerSignature.sqlKinds())
              .forEachOrdered(sqlKind -> builder.put(sqlKind, emptyConstructor));
        } catch (Exception e) {
          LOGGER
              .atError()
              .addArgument(klass.getName())
              .addArgument(e)
              .log("Failed to find no arg constructor for {}");
          throw RisingWaveException.from(ExecutionError.INTERNAL, e);
        }
      }
    }

    return builder.build();
  }
}
