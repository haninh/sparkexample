package vn.wss.wordsearch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtils {
	public static String formatString(String s) {
		return s.replaceAll(" ", "+");
	}

	public static String getWordSearch(String s) {
		String regex = ".*\\/s\\/(.*)\\.htm";
		String regex_1 = "cat-[0-9]*\\/(.*)";
		String regex_2 = "(.*)\\/.*";
		Pattern f1 = Pattern.compile(regex);
		Pattern filter1 = Pattern.compile(regex_1);
		Pattern filter2 = Pattern.compile(regex_2);
		Matcher m = f1.matcher(s);
		if (m.matches()) {
			String s1 = m.group(1);
			Matcher m_f1 = filter1.matcher(s1);
			if (m_f1.matches()) {
				s1 = m_f1.group(1);
			}
			Matcher m_f2 = filter2.matcher(s1);
			while (m_f2.matches()) {
				s1 = m_f2.group(1);
				m_f2 = filter2.matcher(s1);
			}
			return s1.toLowerCase();
		}
		return null;
	}
}
