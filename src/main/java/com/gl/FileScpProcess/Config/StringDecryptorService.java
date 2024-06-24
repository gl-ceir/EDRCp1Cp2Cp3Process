/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.gl.FileScpProcess.Config;

/**
 *
 * @author maverick
 */
import org.jasypt.encryption.StringEncryptor;
import org.jasypt.util.text.BasicTextEncryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class StringDecryptorService {

    private final StringEncryptor stringEncryptor;

    @Autowired
    public StringDecryptorService(StringEncryptor stringEncryptor) {
        this.stringEncryptor = stringEncryptor;
    }

    public String decrypt(String encryptedText) {
        return stringEncryptor.decrypt(encryptedText);
    }

    public String decryptor(String encryptedText) {
        BasicTextEncryptor encryptor = new BasicTextEncryptor();
        String secretKey = System.getenv("JASYPT_ENCRYPTOR_PASSWORD");
        encryptor.setPassword(secretKey);
        return encryptor.decrypt(encryptedText);
    }
}
