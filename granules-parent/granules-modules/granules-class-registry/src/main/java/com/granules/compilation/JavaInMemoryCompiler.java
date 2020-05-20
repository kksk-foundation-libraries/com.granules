package com.granules.compilation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.tools.DiagnosticCollector;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.ErrorHandler;

public class JavaInMemoryCompiler extends ErrorHandler {
	private static final Logger LOG = LoggerFactory.getLogger(JavaInMemoryCompiler.class);

	private static final JavaInMemoryCompiler INSTANCE = new JavaInMemoryCompiler();

	private final ClassLoader cl = ClassLoader.getSystemClassLoader();
	private final Method defineClassMethod;
	private final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
	private final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
	private final StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(diagnostics, null, null);
	private final ConcurrentMap<String, JavaFileObject> volatileMap = new ConcurrentHashMap<>();
	private final JavaFileManager fileManager = new ForwardingJavaFileManager<StandardJavaFileManager>(standardFileManager) {
		@Override
		public JavaFileObject getJavaFileForOutput(Location location, String className, JavaFileObject.Kind kind, FileObject sibling) throws IOException {
			return volatileMap.get(className);
		}
	};

	private JavaInMemoryCompiler() {
		super(null, null);
		Method method = null;
		try {
			method = ClassLoader.class.getDeclaredMethod("defineClass", String.class, byte[].class, Integer.TYPE, Integer.TYPE);
			method.setAccessible(true);
		} catch (Exception e) {
			onWarnning(e);
		}
		defineClassMethod = method;
	}

	public static Class<?> compile(String fqcn, String source) throws Exception {
		return INSTANCE.compile0(fqcn, source);
	}

	private synchronized Class<?> compile0(String fqcn, String source) throws Exception {
		try {
			final ByteArrayOutputStream baos = new ByteArrayOutputStream();
			final SimpleJavaFileObject output = new SimpleJavaFileObject(URI.create("bytes:///" + fqcn.replaceAll("\\.", "/")), Kind.CLASS) {
				ByteArrayOutputStream _baos = baos;

				@Override
				public OutputStream openOutputStream() throws IOException {
					return _baos;
				}
			};
			volatileMap.put(fqcn, output);

			JavaCompiler.CompilationTask task = null;
			task = compiler.getTask(null, fileManager, diagnostics, null, null, Arrays.asList(new SimpleJavaFileObject(URI.create("string:///" + fqcn.replaceAll("\\.", "/") + Kind.SOURCE.extension), Kind.SOURCE) {
				@Override
				public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
					return source;
				}
			}));
			if (!task.call()) {
				diagnostics.getDiagnostics().stream().map(o -> o.toString()).forEach(LOG::info);
			}
			fileManager.flush();
			volatileMap.remove(fqcn);

			byte[] data = baos.toByteArray();
			defineClassMethod.invoke(cl, fqcn, data, 0, data.length);
			Class<?> clazz = cl.loadClass(fqcn);
			return clazz;
		} catch (Exception e) {
			onError(e);
			throw e;
		}
	}
}
