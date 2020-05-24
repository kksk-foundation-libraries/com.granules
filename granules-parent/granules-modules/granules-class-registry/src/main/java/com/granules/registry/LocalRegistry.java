package com.granules.registry;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.granules.ErrorHandler;
import com.granules.compilation.JavaInMemoryCompiler;

public class LocalRegistry extends ErrorHandler {
	private static final Logger LOG = LoggerFactory.getLogger(LocalRegistry.class);

	private static final LocalRegistry INSTANCE = new LocalRegistry();
	private final ConcurrentMap<Integer, Class<?>> classRegistry = new ConcurrentHashMap<>();
	private final LinkedHashMap<Integer, Constructor<?>> constructorCache = new LinkedHashMap<>(10000, 1.0f, true);
	private final ConcurrentMap<String, Integer> idMap = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, String> fqcnMap = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, String> sourceMap = new ConcurrentHashMap<>();

	private LocalRegistry() {
		super(null, null);
	}

	public static void registerSource(Integer id, String fqcn, String source) {
		INSTANCE.registerSource0(id, fqcn, source);
	}

	private void registerSource0(Integer id, String fqcn, String source) {
		sourceMap.put(id, source);
		fqcnMap.put(id, fqcn);
		idMap.put(fqcn, id);
	}

	public static Class<?> loadClass(String fqcn) {
		if (!INSTANCE.idMap.containsKey(fqcn)) {
			return null;
		}
		return INSTANCE.loadClass0(INSTANCE.idMap.get(fqcn));
	}

	public static Class<?> loadClass(Integer id) {
		return INSTANCE.loadClass0(id);
	}

	private Class<?> loadClass0(Integer id) {
		if (!fqcnMap.containsKey(id)) {
			return null;
		}
		return classRegistry.computeIfAbsent(id, _id -> {
			try {
				return Class.forName(fqcnMap.get(_id));
			} catch (ClassNotFoundException ignore) {
			}
			try {
				return JavaInMemoryCompiler.compile(fqcnMap.get(_id), sourceMap.get(_id));
			} catch (Exception e) {
				onError(e);
				return null;
			}
		});
	}

	public static <T> T newInstance(String fqcn) throws Exception {
		if (!INSTANCE.idMap.containsKey(fqcn)) {
			return null;
		}
		return INSTANCE.newInstance0(INSTANCE.idMap.get(fqcn));
	}

	public static <T> T newInstance(Integer id) throws Exception {
		return INSTANCE.newInstance0(id);
	}

	@SuppressWarnings("unchecked")
	private <T> T newInstance0(Integer id) throws Exception {
		if (!fqcnMap.containsKey(id)) {
			return null;
		}
		return (T) constructorCache.computeIfAbsent(id, _id -> {
			try {
				return loadClass0(_id).getConstructor();
			} catch (Exception e) {
				LOG.error("could not create constructor:" + _id, e);
				onError(e);
				return null;
			}
		}).newInstance();
	}
}
