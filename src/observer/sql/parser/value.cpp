/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2023/06/28.
//

#include "sql/parser/value.h"
#include "common/lang/comparator.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include <sstream>
#include "value.h"

const char *ATTR_TYPE_NAME[] = {
    "undefined",
    "chars",
    "ints",
    "dates",
    "floats",
    "booleans",
};

const char *attr_type_to_string(AttrType type)
{
  if (type >= UNDEFINED && type <= FLOATS) {
    return ATTR_TYPE_NAME[type];
  }
  return "unknown";
}
AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return UNDEFINED;
}

Value::Value(int val) { set_int(val); }

Value::Value(float val) { set_float(val); }

Value::Value(bool val) { set_boolean(val); }

Value::Value(const char *s, int len /*= 0*/) { set_string(s, len); }

Value::Value(const char *date, int len, int flag)
{
  int p1 = 0, p2 = 0;  // 记录两个间隔符的位置
  for (int i = 0; i < len; i++) {
    if (date[i] == '-') {
      if (p1 == 0)
        p1 = i;
      else if (p2 == 0) {
        p2 = i;
        break;
      }
    }
  }
  int year = 0, month = 0, day = 0;
  for (int i = 0; i < p1; i++) {
    year *= 10;
    year += date[i] - '0';
  }
  for (int i = p1 + 1; i < p2; i++) {
    month *= 10;
    month += date[i] - '0';
  }
  for (int i = p2 + 1; i < len; i++) {
    day *= 10;
    day += date[i] - '0';
  }
  // std::cout << year << ' ' << month << ' ' << day << std::endl;
  // 闰年打表
  static int leap_year[17] = {
      1972, 1976, 1980, 1984, 1988, 1992, 1996, 2000, 2004, 2008, 2012, 2016, 2020, 2024, 2028, 2032, 2036};
  static int month_len[12]      = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  static int month_len_leap[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int        leap_num           = 17;
  for (int i = 0; i < 17; i++) {
    if (year <= leap_year[i]) {
      leap_num = i;
      break;
    }
  }
  int  val   = 0;
  bool valid = false;
  bool leap  = false;
  if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))
    leap = true;
  // 对年的计算
  if (year < 1970 || year > 2038)
    val = -1e8;
  else if (month < 1 || month > 12) {
    val = -1e8;
    // std::cout << "month invalid" << std::endl;
  } else if ((day < 1) || (!leap && day > month_len[month - 1]) || (leap && day > month_len_leap[month - 1]))
    val = -1e8;
  else {
    valid = true;
  }
  if (valid) {

    val += (year - 1970 - leap_num) * 365 + leap_num * 366;

    // 对月的计算
    for (int i = 0; i < month - 1; i++) {
      val += month_len[i];
    }
    if (((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) && month > 2) {  // 闰年并且大于2月
      val += 1;
    }

    // 对日的计算
    val += day - 1;
  }
  if (valid && val >= 0 && val <= 24867)
    set_date(val);
}

void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case CHARS: {
      set_string(data, length);
    } break;
    case INTS: {
      num_value_.int_value_ = *(int *)data;
      length_               = length;
    } break;
    case FLOATS: {
      num_value_.float_value_ = *(float *)data;
      length_                 = length;
    } break;
    case BOOLEANS: {
      num_value_.bool_value_ = *(int *)data != 0;
      length_                = length;
    } break;
    case DATES: {
      num_value_.date_value_ = *(int *)data;
      length_                = length;
    }
    default: {
      LOG_WARN("unknown data type: %d", attr_type_);
    } break;
  }
}
void Value::set_int(int val)
{
  attr_type_            = INTS;
  num_value_.int_value_ = val;
  length_               = sizeof(val);
}

void Value::set_float(float val)
{
  attr_type_              = FLOATS;
  num_value_.float_value_ = val;
  length_                 = sizeof(val);
}
void Value::set_boolean(bool val)
{
  attr_type_             = BOOLEANS;
  num_value_.bool_value_ = val;
  length_                = sizeof(val);
}
void Value::set_string(const char *s, int len /*= 0*/)
{
  attr_type_ = CHARS;
  if (len > 0) {
    len = strnlen(s, len);
    str_value_.assign(s, len);
  } else {
    str_value_.assign(s);
  }
  length_ = str_value_.length();
}

void Value::set_date(int val)
{
  attr_type_             = DATES;
  num_value_.date_value_ = val;
  length_                = sizeof(val);
}

void Value::set_value(const Value &value)
{
  switch (value.attr_type_) {
    case INTS: {
      set_int(value.get_int());
    } break;
    case FLOATS: {
      set_float(value.get_float());
    } break;
    case CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case DATES: {
      set_date(value.get_date());
    }
    case UNDEFINED: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

const char *Value::data() const
{
  switch (attr_type_) {
    case CHARS: {
      return str_value_.c_str();
    } break;
    default: {
      return (const char *)&num_value_;
    } break;
  }
}

std::string Value::to_string() const
{
  std::stringstream os;
  switch (attr_type_) {
    case INTS: {
      os << num_value_.int_value_;
    } break;
    case FLOATS: {
      os << common::double_to_str(num_value_.float_value_);
    } break;
    case BOOLEANS: {
      os << num_value_.bool_value_;
    } break;
    case CHARS: {
      os << str_value_;
    } break;
    case DATES: {
      int              val  = num_value_.date_value_;
      int              tmp  = 0;
      int              year = 1970, month = 1, day = 1;
      static const int daysInMonthNonLeap[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
      static const int daysInMonthLeap[]    = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
      while (tmp < val) {
        tmp += 365 + ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) * 1;
        year++;
      }
      if (tmp == val) {
        os << year << '-';
        if (month < 10)
          os << 0;
        os << month << '-';
        if (day < 10)
          os << 0;
        os << day;
      } else {
        year--;
        tmp -= 365 + ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) * 1;
        if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
          while (tmp < val) {
            tmp += daysInMonthLeap[month - 1];
            month++;
          }
        } else {
          while (tmp < val) {
            tmp += daysInMonthNonLeap[month - 1];
            month++;
          }
        }
        if (tmp == val) {
          os << year << '-';
          if (month < 10)
            os << 0;
          os << month << '-';
          if (day < 10)
            os << 0;
          os << day;
        } else {
          month--;
          if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
            tmp -= daysInMonthLeap[month - 1];
          } else {
            tmp -= daysInMonthNonLeap[month - 1];
          }
          day += val - tmp;
          os << year << '-';
          if (month < 10)
            os << 0;
          os << month << '-';
          if (day < 10)
            os << 0;
          os << day;
        }
      }
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
  return os.str();
}

int Value::compare(const Value &other) const
{
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS: {
        return common::compare_int((void *)&this->num_value_.int_value_, (void *)&other.num_value_.int_value_);
      } break;
      case FLOATS: {
        return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other.num_value_.float_value_);
      } break;
      case CHARS: {
        return common::compare_string((void *)this->str_value_.c_str(),
            this->str_value_.length(),
            (void *)other.str_value_.c_str(),
            other.str_value_.length());
      } break;
      case DATES: {
        return common::compare_int((void *)&this->num_value_.date_value_, (void *)&other.num_value_.date_value_);
      } break;
      case BOOLEANS: {
        return common::compare_int((void *)&this->num_value_.bool_value_, (void *)&other.num_value_.bool_value_);
      }
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = this->num_value_.int_value_;
    return common::compare_float((void *)&this_data, (void *)&other.num_value_.float_value_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = other.num_value_.int_value_;
    return common::compare_float((void *)&this->num_value_.float_value_, (void *)&other_data);
  }
  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

int Value::get_int() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return (int)(std::stol(str_value_));
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0;
      }
    }
    case INTS: {
      return num_value_.int_value_;
    }
    case FLOATS: {
      return (int)(num_value_.float_value_);
    }
    case DATES: {
      return num_value_.date_value_;
    }
    case BOOLEANS: {
      return (int)(num_value_.bool_value_);
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        return std::stof(str_value_);
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return 0.0;
      }
    } break;
    case INTS: {
      return float(num_value_.int_value_);
    } break;
    case FLOATS: {
      return num_value_.float_value_;
    } break;
    case BOOLEANS: {
      return float(num_value_.bool_value_);
    } break;
    case DATES: {
      return float(num_value_.date_value_);
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

std::string Value::get_string() const { return this->to_string(); }

int Value::get_date() const { return num_value_.date_value_; }

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case CHARS: {
      try {
        float val = std::stof(str_value_);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str_value_);
        if (int_val != 0) {
          return true;
        }

        return !str_value_.empty();
      } catch (std::exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str_value_.c_str(), ex.what());
        return !str_value_.empty();
      }
    } break;
    case INTS: {
      return num_value_.int_value_ != 0;
    } break;
    case FLOATS: {
      float val = num_value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case BOOLEANS: {
      return num_value_.bool_value_;
    } break;
    case DATES: {
      return num_value_.int_value_ >= 0 && num_value_.int_value_ <= 24867;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}
