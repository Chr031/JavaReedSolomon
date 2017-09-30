package com.backblaze.erasure;

import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.backblaze.erasure.ByteErasureService.Part;

public class ByteErasureServiceTest {

	private Random random = new Random(System.nanoTime());

	@Test
	public void test() throws ErasureException {

		final int sampleSize = 1024*1024*100 ;//random.nextInt(1024 * 1024);
		byte[] bytes = new byte[1024 + sampleSize];

		random.nextBytes(bytes);

		ByteErasureService erasure = new ByteErasureService();

		List<Part> parts = erasure.encode(bytes, 12, 6);
		for (int i = 0; i < 6; i++) {
			parts.remove(random.nextInt(parts.size()));
		}
		byte[] decoded = erasure.decode(parts, 12, 6);

		Assert.assertArrayEquals(bytes, decoded);

	}
	
	@Test
	public void testMulti() throws ErasureException {
		int i = 0; while (i++<10) {
			test();
		}
	}
}
