#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# reference from https://segmentfault.com/a/1190000017792696 and support length of 94

import base64
# 第三方库
import rsa
# 系统库
import six

PUB_KEY_STRING = 'MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKHGwq7q2RmwuRgKxBypQHw0mYu4BQZ3eMsTrdK8E6igRcxsobUC7uT0SoxIjl1WveWniCASejoQtn/BY6hVKWsCAwEAAQ=='

class DecryptByPublicKey(object):

    """
        先产生模数因子
        然后生成rsa公钥
        再使用rsa公钥去解密传入的加密str
    """

    def __init__(self, encrypt_text):
        self._encrypt_text = encrypt_text
        self._pub_string_key = PUB_KEY_STRING

        # 使用公钥字符串求出模数和因子

        self._modulus = None   # 模数
        self._exponent = None  # 因子

        # 使用PublicKey(模数,因子)算出公钥

        self._pub_rsa_key = None

    def _gen_modulus_exponent(self, s) ->int:

        # 对字符串解码, 解码成功返回 模数和指数

        b_str = base64.b64decode(s)
        if len(b_str) < 162:
            hex_str = b_str.hex()

            # 找到模数和指数的开头结束位置

            m_start = 25 * 2
            e_start = 91 * 2
            m_len = 64 * 2
            e_len = 3 * 2
            self._modulus = int(hex_str[m_start:m_start + m_len], 16)
            self._exponent = int(hex_str[e_start:e_start + e_len], 16)
        else:
            hex_str = b_str.hex()

            # 找到模数和指数的开头结束位置

            m_start = 29 * 2
            e_start = 159 * 2
            m_len = 128 * 2
            e_len = 3 * 2
            self._modulus = int(hex_str[m_start:m_start + m_len], 16)
            self._exponent = int(hex_str[e_start:e_start + e_len], 16)
            

    def _gen_rsa_pubkey(self):

        # 将pub key string 转换为 pub rsa key

        try:
            rsa_pubkey = rsa.PublicKey(self._modulus, self._exponent)

            # 赋值到_pub_rsa_key

            self._pub_rsa_key = rsa_pubkey.save_pkcs1()
        except Exception as e:
            raise e

    def decode(self) ->str:

        """ 
        decrypt msg by public key
        """

        b64decoded_encrypt_text = base64.b64decode(self._encrypt_text)
        public_key = rsa.PublicKey.load_pkcs1(self._pub_rsa_key)
        encrypted = rsa.transform.bytes2int(b64decoded_encrypt_text)
        decrypted = rsa.core.decrypt_int(encrypted, public_key.e, public_key.n)
        decrypted_bytes = rsa.transform.int2bytes(decrypted)

        # 这里使用了six库的iterbytes()方法去模拟python2对bytes的轮询

        if len(decrypted_bytes) > 0 and list(six.iterbytes(decrypted_bytes))[0] == 1:
            try:
                raw_info = decrypted_bytes[decrypted_bytes.find(b'\x00')+1:]
            except Exception as e:
                raise e
        return raw_info.decode("utf-8")

    def decrypt(self) -> str:

        """
        先产生模数因子
        然后生成rsa公钥
        再使用rsa公钥去解密
        """

        self._gen_modulus_exponent(self._pub_string_key)
        self._gen_rsa_pubkey()
        ret = self.decode()
        return ret
