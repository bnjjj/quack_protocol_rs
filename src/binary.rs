use crate::constants::FIELD_END;
use crate::errors::{QuackError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HugeIntParts {
    pub(crate) upper: i64,
    pub(crate) lower: u64,
}

impl std::fmt::Display for HugeIntParts {
    /// Formats the value as a UUID string
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // DuckDB stores UUIDs as HUGEINT with the most significant bit flipped;
        // flip it back so the output matches the server-side UUID string.
        let upper = (self.upper as u64) ^ (1u64 << 63);
        write!(
            f,
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            upper >> 32,
            (upper >> 16) & 0xffff,
            upper & 0xffff,
            self.lower >> 48,
            self.lower & 0x0000_ffff_ffff_ffff
        )
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BinaryWriter {
    buffer: Vec<u8>,
}

impl BinaryWriter {
    pub(crate) fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(1024),
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.buffer
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    pub(crate) fn write_object(
        &mut self,
        write: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        write(self)?;
        self.write_field_id(FIELD_END)
    }

    pub(crate) fn write_field(
        &mut self,
        field_id: u16,
        write: impl FnOnce(&mut Self) -> Result<()>,
    ) -> Result<()> {
        self.write_field_id(field_id)?;
        write(self)
    }

    pub(crate) fn write_field_id(&mut self, field_id: u16) -> Result<()> {
        self.buffer.extend_from_slice(&field_id.to_le_bytes());
        Ok(())
    }

    pub(crate) fn write_byte(&mut self, value: u8) -> Result<()> {
        self.buffer.push(value);
        Ok(())
    }

    pub(crate) fn write_bytes(&mut self, value: &[u8]) -> Result<()> {
        self.buffer.extend_from_slice(value);
        Ok(())
    }

    pub(crate) fn write_bool(&mut self, value: bool) -> Result<()> {
        self.write_byte(if value { 1 } else { 0 })
    }

    pub(crate) fn write_uleb(&mut self, value: impl Into<u128>) -> Result<()> {
        let mut current = value.into();
        while current >= 0x80 {
            self.write_byte(((current & 0x7f) as u8) | 0x80)?;
            current >>= 7;
        }
        self.write_byte(current as u8)
    }

    pub(crate) fn write_sleb(&mut self, value: impl Into<i128>) -> Result<()> {
        let mut current = value.into();
        loop {
            let mut byte = (current & 0x7f) as u8;
            current >>= 7;
            let sign_bit_set = (byte & 0x40) != 0;
            let done = (current == 0 && !sign_bit_set) || (current == -1 && sign_bit_set);
            if !done {
                byte |= 0x80;
            }
            self.write_byte(byte)?;
            if done {
                return Ok(());
            }
        }
    }

    pub(crate) fn write_string(&mut self, value: &str) -> Result<()> {
        self.write_string_bytes(value.as_bytes())
    }

    pub(crate) fn write_string_bytes(&mut self, value: &[u8]) -> Result<()> {
        self.write_uleb(value.len() as u128)?;
        self.write_bytes(value)
    }

    pub(crate) fn write_blob(&mut self, value: &[u8]) -> Result<()> {
        self.write_uleb(value.len() as u128)?;
        self.write_bytes(value)
    }

    pub(crate) fn write_list<T>(
        &mut self,
        items: &[T],
        mut write_element: impl FnMut(&mut Self, &T, usize) -> Result<()>,
    ) -> Result<()> {
        self.write_uleb(items.len() as u128)?;
        for (index, item) in items.iter().enumerate() {
            write_element(self, item, index)?;
        }
        Ok(())
    }

    pub(crate) fn write_nullable<T>(
        &mut self,
        value: Option<&T>,
        write_value: impl FnOnce(&mut Self, &T) -> Result<()>,
    ) -> Result<()> {
        match value {
            Some(value) => {
                self.write_bool(true)?;
                write_value(self, value)
            }
            None => self.write_bool(false),
        }
    }

    pub(crate) fn write_huge_int_parts(&mut self, value: HugeIntParts) -> Result<()> {
        self.write_sleb(value.upper as i128)?;
        self.write_uleb(value.lower as u128)
    }

    pub(crate) fn write_fixed_i8(&mut self, value: i8) -> Result<()> {
        self.write_byte(value as u8)
    }

    pub(crate) fn write_fixed_u8(&mut self, value: u8) -> Result<()> {
        self.write_byte(value)
    }

    pub(crate) fn write_fixed_i16(&mut self, value: i16) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_u16(&mut self, value: u16) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_i32(&mut self, value: i32) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_u32(&mut self, value: u32) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_i64(&mut self, value: i64) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_u64(&mut self, value: u64) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_f32(&mut self, value: f32) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }

    pub(crate) fn write_fixed_f64(&mut self, value: f64) -> Result<()> {
        self.write_bytes(&value.to_le_bytes())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BinaryReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BinaryReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub(crate) fn eof(&self) -> bool {
        self.remaining() == 0
    }

    pub(crate) fn assert_eof(&self) -> Result<()> {
        if self.eof() {
            Ok(())
        } else {
            Err(QuackError::protocol(format!(
                "unexpected trailing bytes at offset {}",
                self.offset
            )))
        }
    }

    pub(crate) fn read_object<T>(
        &mut self,
        read: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let result = read(self)?;
        self.read_end_object()?;
        Ok(result)
    }

    pub(crate) fn read_end_object(&mut self) -> Result<()> {
        let field_id = self.read_field_id()?;
        if field_id != FIELD_END {
            return Err(QuackError::protocol(format!(
                "expected end of object at offset {}, got field {}",
                self.offset - 2,
                field_id
            )));
        }
        Ok(())
    }

    pub(crate) fn read_field_id(&mut self) -> Result<u16> {
        self.ensure(2)?;
        let value = u16::from_le_bytes([self.bytes[self.offset], self.bytes[self.offset + 1]]);
        self.offset += 2;
        Ok(value)
    }

    pub(crate) fn peek_field_id(&self) -> Result<u16> {
        self.ensure(2)?;
        Ok(u16::from_le_bytes([
            self.bytes[self.offset],
            self.bytes[self.offset + 1],
        ]))
    }

    pub(crate) fn read_required_field<T>(
        &mut self,
        field_id: u16,
        read: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let actual = self.read_field_id()?;
        if actual != field_id {
            return Err(QuackError::protocol(format!(
                "expected field {} at offset {}, got {}",
                field_id,
                self.offset - 2,
                actual
            )));
        }
        read(self)
    }

    pub(crate) fn read_optional_field<T>(
        &mut self,
        field_id: u16,
        read: impl FnOnce(&mut Self) -> Result<T>,
        default_value: T,
    ) -> Result<T> {
        if self.peek_field_id()? != field_id {
            return Ok(default_value);
        }
        self.read_field_id()?;
        read(self)
    }

    pub(crate) fn read_byte(&mut self) -> Result<u8> {
        self.ensure(1)?;
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    pub(crate) fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>> {
        self.ensure(length)?;
        let start = self.offset;
        self.offset += length;
        Ok(self.bytes[start..self.offset].to_vec())
    }

    pub(crate) fn read_bool(&mut self) -> Result<bool> {
        match self.read_byte()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(QuackError::protocol(format!(
                "invalid boolean byte {}",
                value
            ))),
        }
    }

    pub(crate) fn read_uleb_u128(&mut self) -> Result<u128> {
        let mut result = 0u128;
        let mut shift = 0u32;
        for _ in 0..19 {
            let byte = self.read_byte()?;
            result |= ((byte & 0x7f) as u128) << shift;
            if (byte & 0x80) == 0 {
                return Ok(result);
            }
            shift += 7;
        }
        Err(QuackError::protocol("unsigned LEB128 value is too long"))
    }

    pub(crate) fn read_uleb_u64(&mut self) -> Result<u64> {
        u64::try_from(self.read_uleb_u128()?)
            .map_err(|_| QuackError::protocol("unsigned LEB128 value exceeds u64"))
    }

    pub(crate) fn read_uleb_usize(&mut self) -> Result<usize> {
        usize::try_from(self.read_uleb_u128()?)
            .map_err(|_| QuackError::protocol("unsigned LEB128 value exceeds usize"))
    }

    pub(crate) fn read_sleb_i128(&mut self) -> Result<i128> {
        let mut result = 0i128;
        let mut shift = 0u32;
        for _ in 0..19 {
            let byte = self.read_byte()?;
            result |= ((byte & 0x7f) as i128) << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                if (byte & 0x40) != 0 {
                    result |= (-1i128) << shift;
                }
                return Ok(result);
            }
        }
        Err(QuackError::protocol("signed LEB128 value is too long"))
    }

    pub(crate) fn read_sleb_i64(&mut self) -> Result<i64> {
        i64::try_from(self.read_sleb_i128()?)
            .map_err(|_| QuackError::protocol("signed LEB128 value exceeds i64"))
    }

    pub(crate) fn read_string(&mut self) -> Result<String> {
        String::from_utf8(self.read_string_bytes()?).map_err(Into::into)
    }

    pub(crate) fn read_string_bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.read_uleb_usize()?;
        self.read_bytes(length)
    }

    pub(crate) fn read_blob(&mut self) -> Result<Vec<u8>> {
        let length = self.read_uleb_usize()?;
        self.read_bytes(length)
    }

    pub(crate) fn read_list<T>(
        &mut self,
        mut read_element: impl FnMut(&mut Self, usize) -> Result<T>,
    ) -> Result<Vec<T>> {
        let length = self.read_uleb_usize()?;
        let mut result = Vec::with_capacity(length);
        for index in 0..length {
            result.push(read_element(self, index)?);
        }
        Ok(result)
    }

    pub(crate) fn read_nullable<T>(
        &mut self,
        read_value: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<Option<T>> {
        if self.read_bool()? {
            Ok(Some(read_value(self)?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn read_huge_int_parts(&mut self) -> Result<HugeIntParts> {
        let upper = self.read_sleb_i64()?;
        let lower = self.read_uleb_u64()?;
        Ok(HugeIntParts { upper, lower })
    }

    pub(crate) fn read_fixed_i8(&mut self) -> Result<i8> {
        Ok(self.read_byte()? as i8)
    }

    pub(crate) fn read_fixed_u8(&mut self) -> Result<u8> {
        self.read_byte()
    }

    pub(crate) fn read_fixed_i16(&mut self) -> Result<i16> {
        let bytes = self.read_array::<2>()?;
        Ok(i16::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_u16(&mut self) -> Result<u16> {
        let bytes = self.read_array::<2>()?;
        Ok(u16::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_i32(&mut self) -> Result<i32> {
        let bytes = self.read_array::<4>()?;
        Ok(i32::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_u32(&mut self) -> Result<u32> {
        let bytes = self.read_array::<4>()?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_i64(&mut self) -> Result<i64> {
        let bytes = self.read_array::<8>()?;
        Ok(i64::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_u64(&mut self) -> Result<u64> {
        let bytes = self.read_array::<8>()?;
        Ok(u64::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_f32(&mut self) -> Result<f32> {
        let bytes = self.read_array::<4>()?;
        Ok(f32::from_le_bytes(bytes))
    }

    pub(crate) fn read_fixed_f64(&mut self) -> Result<f64> {
        let bytes = self.read_array::<8>()?;
        Ok(f64::from_le_bytes(bytes))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure(N)?;
        let mut bytes = [0u8; N];
        bytes.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(bytes)
    }

    fn ensure(&self, length: usize) -> Result<()> {
        if self.offset + length > self.bytes.len() {
            return Err(QuackError::protocol(format!(
                "unexpected end of input at offset {}; needed {} byte(s), have {}",
                self.offset,
                length,
                self.remaining()
            )));
        }
        Ok(())
    }
}

pub(crate) fn split_signed_huge_int(value: i128) -> HugeIntParts {
    HugeIntParts {
        upper: (value >> 64) as i64,
        lower: value as u64,
    }
}

pub(crate) fn combine_signed_huge_int(parts: HugeIntParts) -> i128 {
    ((parts.upper as i128) << 64) | (parts.lower as i128)
}

pub(crate) fn combine_unsigned_huge_int(parts: HugeIntParts) -> u128 {
    ((parts.upper as u64 as u128) << 64) | parts.lower as u128
}
