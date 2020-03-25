package ru.datana.kafka.gateway.utils;

import lombok.Getter;
import lombok.ToString;

/**
 * Вид Exception для внешеного потребителея
 */
@ToString
@Getter
public enum TypeException {

    OK(0, "Успешно"),
    SYSTEM_ERROR(-1, "Системная ошибка"),
    INVALID_USER_INPUT_DATA(-2, "Не корректные введенные данные"),
    XML_TO_TEXT_ERROR(-1000, "Не верный входной XML"),
    ROOT_XML_DUPLICATE_LINES_ERROR(-1010, "Не верный входной Root XML (config-plc.xml) - дублирование строк"),
    ROOT_XML_NULL_ATTRIBUTE_LINES_ERROR(-1011, "Пустой xml атрубиут у Root XML (config-plc.xml)"),
    DIR_NOT_FOUND(-2000, "Пустая папка под список файлов");
    private final int codeError;
    private final String descError;

    TypeException(int codeError, String descError) {
        this.codeError = codeError;
        this.descError = descError;
    }
}
