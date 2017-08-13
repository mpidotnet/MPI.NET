# Copyright (C) 2007  The Trustees of Indiana University
#
# Use, modification and distribution is subject to the Boost Software
# License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)
#  
# Authors: Douglas Gregor
#          Andrew Lumsdaine
#
# This script customizes the "Unsafe" class by updating the various
# constant values with values from mpi.h. 
# Usage:
#
#   perl Unsafe.pl <mpi-header> <Unsafe.cs> <CustomUnsafe.cs> <cbridge.c>
#
# Parameters:
#   <mpi-header>: the full path of the MPI header, 
#   e.g., usr/include/openmpi/mpi.h 
#
#   <Unsafe.cs>: the Unsafe.cs input file
# 
#   <CustomUnsafe.cs>: the customized Unsafe.cs output file
#
#   <cbridge.c>: the customized C bridge needed to load the MPI.NET assembly
use English;
use Config;

sub print_cbridge_customizations()
{
    my $element;
    foreach $element (@cbridge_constants) {
        my $type;
        my $constant;
        ($type, $constant) = split(/:/, $element);
        print UNSAFE_OUTPUT "        [DllImport(\"mpinet\")] public static unsafe extern $type mpinet_$constant();\n";
        print UNSAFE_OUTPUT "\n";
    }
}

sub parse_enum($) 
{
    my $line = shift;
    my $done = 0;
    while (! $done) {
        if ($line =~ /(MPI_[A-Za-z0-9_]*)\s*=\s*((0x)?[0-9]+)/) {
	    $constants{$1} = $2;
	    $next_enum_value = $2 + 1;
            $line = $POSTMATCH;
        } elsif ($line =~ /(MPI_[A-Za-z0-9_]*)/) {
	    $constants{$1} = $next_enum_value;
	    $next_enum_value++;
            $line = $POSTMATCH;
        } elsif ($line =~ /\}/) {
	    $in_enum = 0; 
            $done = 1;
        } else {
            $done = 1;
        }
    }
}

sub write_cbridge_source()
{
    open (CBRIDGE_OUTPUT, ">$cbridge_output") or die ("Cannot write C bridge output file $cbridge_output");
    print CBRIDGE_OUTPUT 
        ("/* Copyright (C) 2007, 2008  The Trustees of Indiana University\n",
         " *\n",
         " * Use, modification and distribution is subject to the Boost Software\n",
         " * License, Version 1.0. (See accompanying file LICENSE_1_0.txt or copy at\n",
         " * http://www.boost.org/LICENSE_1_0.txt)\n",
         " *\n",
	 " * This file was automatically generated from Unsafe.cs by Unsafe.pl.\n",
	 " * Edit at your own risk.\n",
	 " *\n",
         " * Authors: Douglas Gregor\n",
         " *          Andrew Lumsdaine\n",
         " */\n",
         "#include <mpi.h>\n",
         "\n");

    foreach $element (@cbridge_constants) {
        my $type;
        my $constant;
        ($type, $constant) = split(/:/, $element);

	if ($constant =~ /_FN$/) {
	    # The _FN constants are function types an C, so
	    # our C bridge needs to return pointers to those
	    # function types. In C#, they are delegates, so we
	    # don't have the pointers.
	    $type = $type . " *";
	}

        print CBRIDGE_OUTPUT "$type mpinet_$constant() { return $constant; }\n";
    }
    close CBRIDGE_OUTPUT or die ("Could not write output file $cbridge_output");
}

# Looking for -DSIZEOF_INT etc. in command line arguments.  We need
# this to convert between C and C# sizes correctly.
$args = join(' ', @ARGV);
if ($args =~ /SIZEOF_INT=([0-9]+)/)
{
    $SIZEOF_INT=$1;
}
if ($args =~ /SIZEOF_LONG=([0-9]+)/)
{
    $SIZEOF_LONG=$1;
}
if ($args =~ /SIZEOF_LONG_LONG=([0-9]+)/)
{
    $SIZEOF_LONG_LONG=$1;
}
if ($args =~ /SIZEOF_SIZE_T=([0-9]+)/)
{
    $SIZEOF_SIZE_T=$1;
}

# Set sizes based on the arguments we were given on the command line.
unless ($SIZEOF_INT ne '') 
{
    $SIZEOF_INT = 4;
    $SIZEOF_LONG = 8;
    $SIZEOF_LONG_LONG = 8;
    $SIZEOF_SIZE_T = 8;
}
%csharp_signed_types = (4 => 'int',
			8 => 'long');
%csharp_unsigned_types = (4 => 'uint',
			  8 => 'ulong');
# A hash that stores the C# types corresponding to C types
%var_type_subs = ('int'                => $csharp_signed_types{$SIZEOF_INT},
		  'unsigned'           => $csharp_unsigned_types{$SIZEOF_INT},
		  'unsigned int'       => $csharp_unsigned_types{$SIZEOF_INT},
		  'long'               => $csharp_signed_types{$SIZEOF_LONG},
		  'unsigned long'      => $csharp_unsigned_types{$SIZEOF_LONG},
		  'long long'          => $csharp_signed_types{$SIZEOF_LONG_LONG},
		  'unsigned long long' => $csharp_unsigned_types{$SIZEOF_LONG_LONG},
		  'size_t'             => $csharp_unsigned_types{$SIZEOF_SIZE_T});

# Grab the command-line arguments
$mpi_header = (@ARGV > 3) ? shift @ARGV : "/usr/include/mpi.h";
$unsafe_input = shift @ARGV;
$unsafe_output = shift @ARGV;
$cbridge_output = shift @ARGV;

# A hash that will store the mapping from MPI constants to their values.
%constants = ();

# The fields of MPI_Status
@mpi_status_fields = ();

# Parse the mpi.h header to determine how to map from C# into C MPI
$mpi_status_name="MPI_Status";

print STDERR "MPI header: $mpi_header\n";

load_mpi_header:
$in_mpi_status=0;
$in_enum=0;
open (HEADER,"<$mpi_header") or die ("Cannot open MPI header $mpi_header");
while(defined($line = <HEADER>)) {
    if ($in_mpi_status) {
        # print STDOUT "We are in the struct def : $line\n";
        # We're in the body of the MPI_Status structure (or its alias)
        if ($line =~ /^\s*\}.*;.*$/) {
            $in_mpi_status=0;
            # print STDOUT "Found end of struct definition : $line\n";
        } elsif ($line =~ /([A-Za-z_][A-Za-z0-9_]*)\s+([A-Za-z_][A-Za-z0-9_]*)[^;]*;/) {
            my $type = $1;
            my $name = $2;
            my $access = "internal";
            if ($name eq "MPI_SOURCE" 
                || $name eq "MPI_TAG" 
                || $name eq "MPI_ERROR") {
                $access = "public";
            }
            if ($type eq "size_t") { # this is found in OpenMPI's mpi.h 
                $type = "UIntPtr";
            }
            push(@mpi_status_fields, "            $access $type $name;\n");
            # print STDOUT "Found struct member definition : $line\n";
        }
    } elsif ($line =~ /\s*#define\s+(OPAL_[A-Za-z0-9_]*)\s*(.*)/) { # this is found in OpenMPI's mpi.h 
        # print STDOUT "Found OPAL line: $line";
        # Found an OPAL_* constant defined by the preprocessor
        my $name = $1;
        my $value = $2;
        if ($value =~ /\/\*/) {
            $value = $PREMATCH;
        }
	    $constants{$name} = $value;
    } elsif ($line =~ /\s*#define\s+(MPI_[A-Za-z0-9_]*)\s*(.*)/) {
        # print STDOUT "Found MPI_ constant: $line";
        # Found an MPI_* constant defined by the preprocessor
        my $name = $1;
        my $value = $2;
	    if (exists $constants{$value}) {
		    # This is a constant we need to replace, for instance when hitting 
		    #define MPI_MAX_PROCESSOR_NAME OPAL_MAX_PROCESSOR_NAME
		    $value = $constants{$value};
	    }
        elsif ($value =~ /\/\*/) {
            $value = $PREMATCH;
		    # Cannot seem to get the above working for OPAL_ constants... Reverting to ugly
#		    if ($value eq "OPAL_MAX_PROCESSOR_NAME") { 
#			    $value = "256";
#		    }
#		    if ($value eq "OPAL_MAX_ERROR_STRING") { 
#			    $value = "256";
#		    }
        }
	    $constants{$name} = $value;
    } elsif ($line =~ /^enum/) {
        # print STDOUT "Found enum: $line";
        $in_enum = 1;
        $next_enum_value = 0;
        parse_enum($POSTMATCH);
    } elsif ($in_enum) {
        # print STDOUT "in enum: $line";
        parse_enum($line);
    } elsif ($line =~ /typedef\s+struct\s+([A-Za-z_][A-Za-z0-9_]*)\s+MPI_Status/) {
        # print STDOUT "Found typedef struct MPI_Status: $line";
        $mpi_status_name = $1;
        # # print STDOUT "Found in \"$line\" struct name=\"$mpi_status_name\" \n";
    } elsif ($line =~ /^struct\s+$mpi_status_name[^;]*$/) {
        $in_mpi_status=1;
        # print STDOUT "Found the start of struct definition at $line";
    }
}
close (HEADER);
$in_mpi_status=0;

if ($mpi_status_name != "MPI_Status" && $#mpi_status_fields == -1) {
    goto load_mpi_header;
}

# Keeps track of all of the MPI constants that need to be retrieved
# directly via the MPI.NET CBridge. Each element is a string
# TYPE:CONSTANT.
@cbridge_constants = ();

# Transform the C# "Unsafe" source file (built for MS-MPI) into a
# version customized for this particular MPI
$in_mpi_status=0;
open (UNSAFE_INPUT, "<$unsafe_input") or die ("Cannot load Unsafe.cs input file $unsafe_input");
open (UNSAFE_OUTPUT, ">$unsafe_output") or die ("Cannot write CustomUnsafe.cs output file $unsafe_output");
while(defined($line = <UNSAFE_INPUT>)) {
    if ($in_mpi_status && $line =~ /^\s*}\s*$/) {
        # This is the end of the MPI_Status structure; stop
        $in_mpi_status = 0;
    } elsif ($in_mpi_status) {
        # Do nothing: the contents of MPI_Status from the input file
        # do not pass through.
    } elsif ($line =~ /(\s*)public.*(MPI_[A-Z][a-z0-9_]+\s*[*]?|int) (MPI_[A-Za-z0-9_]+)\s*=.*;/) {
        # Found an MPI_* constant defined in the C# header file
        my $whitespace = $1;
	my $type = $2;
	my $constant = $3;
	if (exists $constants{$constant}) {
	    # This is a constant we need to replace.
	    my $value = $constants{$constant};

	    if ($value =~ /LAM_MPI_C_.+\(FN|NULL|IGNORE\)/) {
		# this is for you LAM :-)
	    	print UNSAFE_OUTPUT ("$whitespace","public static readonly $type $constant = mpinet_$constant();\n");
		push (@cbridge_constants, "$type:$constant");
	    } elsif ($value =~ /OMPI_PREDEFINED_GLOBAL\((.+), (.+)\)/) {
	    	# OpenMPI choose to use external variables for these values
		print UNSAFE_OUTPUT ("$whitespace","public static readonly $type $constant = mpinet_$constant();\n");
		push (@cbridge_constants, "$type:$constant");
	    } elsif ($value =~ /&/ or $constant =~ /_FN$/) {
                # If we're taking the address of something, or if this
                # is a _FN constant, it needs to be done in the C
                # bridge *unless* this is just a fancy name for the
                # NULL pointer.
                if ($value =~ /\(MPI_[A-Z][a-z]*_function\s*[*]\)\s*0/) {
                    print UNSAFE_OUTPUT ("$whitespace","public const $type $constant = null;\n");
                } else {
                    print UNSAFE_OUTPUT ("$whitespace","public static readonly $type $constant = mpinet_$constant();\n");

                    push (@cbridge_constants, "$type:$constant");
                }
	    } elsif ($type =~ /[*]/) {
		# If the type is a pointer type, it can't be const.
		print UNSAFE_OUTPUT ("$whitespace","public static readonly $type $constant = $value;\n");
            } else {
                print UNSAFE_OUTPUT ("$whitespace","public const $type $constant = $value;\n");
            }
	}
    } elsif ($line =~ /public struct MPI_Status/) {
        # We've detected the beginning of the MPI_Status structure in C#.
        # Write out the version we created from mpi.h
        $in_mpi_status = 1;
        print UNSAFE_OUTPUT "        public struct MPI_Status\n";
        print UNSAFE_OUTPUT "        {\n";
        print UNSAFE_OUTPUT @mpi_status_fields;
        print UNSAFE_OUTPUT "        }\n";
    } elsif ($line =~ /CBridge Customizations Follow/) {
        print UNSAFE_OUTPUT $line;
        print_cbridge_customizations();
    } else {
        # Pass the line through unmolested
        print UNSAFE_OUTPUT $line;
    }
}
$in_mpi_status=0;
close (UNSAFE_INPUT);
close UNSAFE_OUTPUT or die ("Could not write output file $unsafe_output");

write_cbridge_source();
