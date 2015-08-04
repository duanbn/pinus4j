/**
 * Copyright 2014 Duan Bingnan
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pinus4j.serializer.codec;

import org.pinus4j.exceptions.CodecException;
import org.pinus4j.serializer.io.DataInput;
import org.pinus4j.serializer.io.DataOutput;

/**
 * 序列化编码接口.
 * 被序列化的对象如果存在父类则会将可以被继承的属性进行序列化，否则将会忽略.每一个具体的编码实现类，在进行编码的时候将当前的标志位进行写入，在解码的时候需要读取标志位然后确定使用哪个一个编码类进行解码.因此每一个具体的解码类只需要读取除标志位之后的数据。<br>
 * 编码的对象分为1.基本类型的序列化，2.自定义对象的序列化，3.集合类的序列化，
 * 每一种类型又分为单个序列化和数组的序列化.对于非基本类型的值还需要判断是否为空<br>
 * <b>基本类型</b>
 * Boolean, Byte, Character, Short, Int, Long, Float, Double<br>
 * 二进制格式<br>
 * 单个：bzw(标志位):byte data:具体值<br>
 * 数组：bzw:byte len:vint data:具体值...<br>
 * <b>自定义对象</b>
 * Object<br>
 * 二进制格式<br>
 * 单个：bzw:byte classname:string field:[基本类型|对象类型]...<br>
 * 数组：bzw:byte classname:string len:vint | data:单个对象...<br>
 *
 * @author duanbn
 * @since 1.0
 */
public interface Codec<T>
{

    public void encode(DataOutput output, T v, CodecConfig config) throws CodecException;

    public T decode(DataInput input, CodecConfig config) throws CodecException;

}
