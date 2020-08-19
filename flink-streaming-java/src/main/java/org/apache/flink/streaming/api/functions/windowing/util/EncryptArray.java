package org.apache.flink.streaming.api.functions.windowing.util;

import org.apache.flink.streaming.api.functions.windowing.ApplyToArrayFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;

/**
 * Array AES encryption. Code adapted from https://gist.github.com/itarato/abef95871756970a9dad
 */

public abstract class EncryptArray<K, W extends Window, T> extends ApplyToArrayFunction<K, W, T, Byte> {

	public ArrayList<Byte> userFunction(ArrayList<T> arr){
		String plainText = "";
		for (T dat : arr){
			plainText += dat.toString();
		}
		byte[] clean = plainText.getBytes();

		// Generating IV.
		int ivSize = 16;
		byte[] iv = new byte[ivSize];
		SecureRandom random = new SecureRandom();
		random.nextBytes(iv);
		IvParameterSpec ivParameterSpec = new IvParameterSpec(iv);

		try {
			// Hashing key.
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			digest.update(keyFunction().getBytes("UTF-8"));
			byte[] keyBytes = new byte[16];
			System.arraycopy(digest.digest(), 0, keyBytes, 0, keyBytes.length);
			SecretKeySpec secretKeySpec = new SecretKeySpec(keyBytes, "AES");

			// Encrypt.
			Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
			cipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, ivParameterSpec);
			byte[] encrypted = cipher.doFinal(clean);

			// Combine IV and encrypted part.
			byte[] encryptedIVAndText = new byte[ivSize + encrypted.length];
			System.arraycopy(iv, 0, encryptedIVAndText, 0, ivSize);
			System.arraycopy(encrypted, 0, encryptedIVAndText, ivSize, encrypted.length);

			ArrayList<Byte> out = new ArrayList<>();
			for (byte b : encryptedIVAndText){
				out.add(b);
			}

			return out;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return new ArrayList<Byte>();
	}

	public String keyFunction(){
		return "";
	}

}
