package tdl.s3.sync;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Filters {

    private final List<Filter> includes = new ArrayList<>();
    
    private final List<Filter> excludes = new ArrayList<>();

    public Filters() {
        exclude(getDefaultLockFilter());
    }
    
    public final void exclude(Filter filter) {
        excludes.add(filter);
    }
    
    public final void include(Filter filter) {
        includes.add(filter);
    }
    
    public boolean accept(Path path) {
        boolean accept = true;
        if (includes.size() > 0) {
            accept = includes.stream()
                .anyMatch(e -> e.accept(path));
        }
        
        boolean reject = excludes.stream()
                .anyMatch(e -> e.accept(path));

        return accept && !reject;
    }
    
    public static final Filter getDefaultLockFilter() {
        return (Path path) -> path.getFileName().toString().endsWith(".lock");
    }
    
    public static final Filter endsWith(String string) {
        return (Path path) -> path.getFileName().toString().endsWith(string);
    }
    
    public static final Filter startsWith(String string) {
        return (Path path) -> path.getFileName().toString().startsWith(string);
    }
    
    public static final Filter matches(String regex) {
        return (Path path) -> path.getFileName().toString().matches(regex);
    }
    
    public static final Filter name(String name) {
        return (Path path) -> path.getFileName().toString().equals(name);
    }
}