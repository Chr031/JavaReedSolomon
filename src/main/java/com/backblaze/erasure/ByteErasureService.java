package com.backblaze.erasure;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteErasureService {

	private static final int BYTES_IN_INT = 4;

	public static class Part {

		private final int shardIndex;

		private final byte[] shardContent;

		public Part(int shardIndex, byte[] shardContent) {
			super();
			this.shardIndex = shardIndex;
			this.shardContent = shardContent;
		}

		public int getShardIndex() {
			return shardIndex;
		}

		public byte[] getShardContent() {
			return shardContent;
		}

	}

	public List<Part> encode(byte[] bytes, int dataShards, int parityShards) {

		int totalShards = dataShards + parityShards;

		// Figure out how big each shard will be. The total size stored
		// will be the file size (4 bytes) plus the file.
		final int storedSize = bytes.length + BYTES_IN_INT;
		final int shardSize = (storedSize + dataShards - 1) / dataShards;

		byte[][] shards = new byte[totalShards][shardSize];
		// first loop with the file size
		shards[0] = new byte[shardSize];
		ByteBuffer.wrap(shards[0]).putInt(bytes.length);
		System.arraycopy(bytes, 0, shards[0], BYTES_IN_INT, shardSize - BYTES_IN_INT);

		// next loops
		for (int i = 1; i < shards.length; i++) {
			shards[i] = new byte[shardSize];
			int rest = bytes.length - (shardSize * i - BYTES_IN_INT);
			if (rest > 0)
				System.arraycopy(bytes, shardSize * i - BYTES_IN_INT, shards[i], 0, Math.min(shardSize, rest));
		}

		// Preparation is done
		// call the erasure itself

		ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
		reedSolomon.encodeParity(shards, 0, shardSize);

		List<Part> parts = new ArrayList<>();
		for (int i = 0; i < shards.length; i++) {
			parts.add(new Part(i, shards[i]));
		}

		return parts;

	}

	public byte[] decode(List<Part> parts, int dataShards, int parityShards) throws ErasureException {

		int totalShards = dataShards + parityShards;

		if (parts.size() < dataShards)
			throw new ErasureException("not enought parts to recover the file");

		int shardSize = parts.get(0).getShardContent().length;

		boolean shardPresent[] = new boolean[totalShards];
		byte[][] shards = new byte[totalShards][shardSize];
		for (Part part : parts) {
			shards[part.getShardIndex()] = part.getShardContent();			
			shardPresent[part.getShardIndex()] = true;
		}

		// decode the erasure
		// Use Reed-Solomon to fill in the missing shards
		ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
		reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

		// Combine the data shards into one buffer for convenience.
		byte[] allBytes = new byte[shardSize * dataShards];
		for (int i = 0; i < dataShards; i++) {
			System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
		}

		// Extract the length of this byte array
		int byteArraySize = ByteBuffer.wrap(allBytes).getInt();

		return Arrays.copyOfRange(allBytes, BYTES_IN_INT, BYTES_IN_INT + byteArraySize);

	}

}
